# Production Pricing Path

This document describes the production pricing path from the data Pricer consumes to the prices, alphas, risk quantities, and order reference prices it publishes into shared cache. It intentionally starts after feed parsing and instrument-map construction: the scope is the `MarketDataObject` stream that Pricer reads and the cache values downstream trading processes consume.

## What This System Is Doing

The trading system prices many exchange instruments that refer to a smaller set of underlying coins. A single coin can have several observable venues/contracts: spot, linear futures, inverse futures, or the same product on several exchanges. The pricing job is to turn that noisy multi-venue stream into:

- one continuously updated fair price per tradeable instrument;
- low/high fair-price envelopes for bid-side and ask-side order logic;
- cross-instrument alphas, separated by time scale and factor structure;
- inventory/risk skews;
- exact reference prices and quantities used by exchange connectors.

The central design is hierarchical:

1. **Instrument stream to local log price**: `SIP` prices one instrument independently from its own quotes/trades.
2. **Instruments to coin fair**: `FPC` combines all active instruments for one coin into a coin fair log price and per-instrument offsets.
3. **Coins to factors/residuals**: `TSPCA` decomposes sampled coin fair returns into market/factor moves and residual moves.
4. **Factors/residuals to alphas**: `SAC` converts those moves into sampled and intra-sample cross-coin alphas.
5. **Fair + alpha + skew to executable references**: `ClintType` writes cancel, retreat, quote, approach, aggression, size, and condition fields to `TradeableDataCache`.

The pricing code does not place orders. It writes shared-memory cache fields. Order-management code reads those fields and decides whether to add, modify, cancel, or cross orders.

## Names And Units

The code uses a few overloaded terms. In this document:

- **Instrument** means an exchange-specific market data stream, identified by `InstrumentID`/uuid. It may be active for pricing even if it is not directly traded.
- **Active slot** means one instrument's position inside the pricing arrays. `IM.active_indexes[InstrumentID]` maps an instrument into `(coin_idx, index_within_coin, raw_index)`.
- **Coin** means the priced underlying index used by FPC/TSPCA/SAC. `NPricing = 48` is the compiled maximum/current expected number of priced coins in the production parameter set.
- **Tradeable** means an instrument that order code can trade. Tradeables are a subset/projection of instruments and are indexed by `tradeable_idx`.
- **SIP** means `SingleInstrumentPricer`, the per-instrument tick model.
- **FPC** means `NewFairPriceCalculator`, the per-coin fair-price combiner.
- **TSPCA** means `OTSPCA`, the online sequential PCA/factor residualizer across coin fair returns.
- **SAC** means `SampledAlphaCalculator`, the sampled and intra-sample alpha engine.
- **PC** means a TSPCA factor/principal component.
- **MDO** means `MarketDataObject`.
- **Cache** means `AllCaches` in shared memory, backed by `Cache_shm`.

Unit conventions matter:

- SIP, FPC, TSPCA, and SAC operate in log-price/log-return space.
- `ActiveDataCache` mostly exposes active-slot diagnostics in log space.
- `CoinDataCache.prices` is coin fair log price.
- `TradeableDataCache.rawprices`, `fairprices`, `loprices`, and `hiprices` are normal prices because `UpdateCachesWithEstimatations` exponentiates the FPC log outputs.
- `TradeableDataCache.alphas`, `skews`, widths, and reference prices are normal price-distance units after multiplication by `fairprices`.
- Time fields are nanoseconds from the system time helpers; constants like `1_MS` are nanoseconds.

The core runtime path is:

1. `Trader/executables/Pricer.cpp` drains `BBO_updates`, a shared-memory queue of `MarketDataObject`.
2. `ClintType::UpdateWithMDO` writes raw market fields into cache and forwards the tick to `FairPriceManager`.
3. `SIP` prices one active instrument stream and produces a log-price plus a local trade-flow alpha.
4. `NewFairPriceCalculator` combines all active instrument prices for one coin into a coin-level fair log price and per-instrument low/high/fair prices.
5. `OTSPCA` periodically turns coin fair-price moves into market/principal-component returns and residuals.
6. `SampledAlphaCalculator` periodically generates sampled cross-coin alphas and continuously accumulates ultra-fast intra-sample alphas.
7. `ClintType::SampledRecalculate` and `ClintType::RecalculateOrderRefPrices` combine fair prices, sampled alphas, intra-sample alphas, position/risk skews, and width parameters into the exact reference prices used by order logic.

All prices inside SIP, FPC, TSPCA, and SAC are log prices or log returns until the cache updater exponentiates per-tradeable outputs. The `TradeableDataCache` price fields exposed to order code are normal prices.

The main files to read alongside this document are:

- `Trader/executables/Pricer.cpp`: process loop and publication cadence.
- `Trader/Clint.hpp`: orchestration, sampled recalculation, order reference prices.
- `Trader/SingleInstrumentPricer.hpp`: SIP.
- `Trader/FairPriceCalculator.hpp`: FPC.
- `Trader/EMPC.hpp`: TSPCA.
- `Trader/SampledAlphaCalculator.hpp`: SAC.
- `Trader/misc/CacheUpdaters.hpp`: cache writes from model outputs.
- `Trader/misc/Caches.hpp`: shared-memory cache schema.
- `Core/KeyDataTypes.hpp`: compile-time dimensions, alpha names, and parameter enum layout.

## Runtime Entry Point

`Pricer` owns:

- `SHMDeverouille<MarketDataObject> BBO_updates{"BBO_updates"}`: input queue.
- `SHMDeverouille<OrderUpdate> OrderUpdates{"OrderUpdates", 14}`: synthetic fill output for trade ticks on tradeable instruments.
- `ClintType Clint`: all pricing model state.

`Pricer` runs as a pinned thread from `main()`. On startup, `ClintType` loads binary params, initializes FPC/SIP/TSPCA/SAC state, and loads per-tradeable parameters into `AllCaches.ParamCache`. From that point on, the loop is intentionally simple: drain market-data queue, update model state, write cache, publish cache.

On every `Pricer::update()` call:

1. `NanoTime()` is read as `tnow`.
2. If `BBO_updates.CheckForFreshData()` reports fresh data, Pricer iterates every queued `MarketDataObject`.
3. Each object is sent to `Clint.UpdateWithMDO(MDO)`.
4. If the object is a trade tick (`MDO.ask == 0`) for a tradeable instrument and notional size exceeds 100, Pricer emits a synthetic `OrderUpdateType::FILL` into `OrderUpdates`. This is separate from fair pricing but uses the same incoming trade event.
5. After the batch, `Clint.RecalculateOrderRefPrices(tnow)` refreshes intra-sample order references.
6. `AllCaches.data_origination_time` is set from the fresh queue element time and `Cache_shm.publish()` publishes the cache.
7. Outside the fresh-data block, Pricer checks one active slot for staleness and calls `Clint.SampledUpdate(tnow)`.

Warmup is stricter. `Pricer::update_warmup()` waits until every active instrument has initialized its SIP from a non-trade L1 quote. During warmup, `Clint.InitWithMDO` calls `SIP::init` and then `FairPriceManager::Calculate<true>` for initialized streams, but it does not write normal estimation cache updates. Once every active SIP has initialized, `FairPriceManager::ResetCalculation()` seeds FPC, TSPCA, sampled alphas, fast SDs, and per-tradeable cache prices; then normal pricing starts.

The cache has a coarse readiness flag. During sampled recalculation `AllCaches.ready` is temporarily set false, then true with release ordering. Most consumers care more about current cache fields than about a transactional snapshot, but the flag marks the sampled recalculation boundary.

## Input Shape Seen By Pricer

The pricing path consumes `MarketDataObject`, inherited from `MarketData`:

```cpp
struct MarketData {
    uint64_t time = 0, exchange_time = 0;
    double bid = 0, ask = 0, bidq = 0, askq = 0;
};

struct MarketDataObject : public MarketData {
    int InstrumentID = -1;
};
```

Production uses two event encodings:

- L1 quote: `ask != 0`. `bid`, `ask`, `bidq`, `askq`, `time`, `exchange_time`, and `InstrumentID` are meaningful.
- Trade tick: `ask == 0`. The code treats `bid` as trade price, `bidq` as signed trade quantity, and `askq` as an update flag. If `askq == 0`, SIP accumulates volume but does not emit a new price sample.

The naming is historical: trade ticks reuse `bid`/`bidq` fields instead of having separate trade-price/trade-qty fields. The sign of `bidq` carries aggressor direction as seen by the upstream data conversion.

`ClintType::UpdateWithMDO` ignores non-active instruments by checking `IM.active_indexes[MDO.InstrumentID]`. For active instruments it first calls `UpdateCachesWithMDO`, then `FairPriceManager::Calculate`.

`UpdateCachesWithMDO` writes only direct market-data fields:

- `TradeableDataCache.bids`, `asks`, `midprices` for quote ticks.
- `ActiveDataCache.bids`, `asks`, `last_tick_time` for the active slot.
- `TradeableDataCache.minimumspreadfrac`, an EWM of whether the current spread is under one tick.

Trade ticks are assigned dummy active/tradeable indexes inside `UpdateCachesWithMDO`, so they do not overwrite normal quote fields. Their pricing effect goes through SIP and FPC.

The input path only uses L1 and trade information. L2 book depth, feed-specific parsing details, and most instrument-map metadata are outside this path.

## SIP: Single-Instrument Pricing

`SIP` lives in `Trader/SingleInstrumentPricer.hpp`. There is one `SIP` per active instrument slot: `FairPriceManager::sips[NPricing * Nfpc]`.

SIP is the first model stage. It observes one instrument's L1 quote and trade events and returns:

```cpp
struct SIP_ret { double price, alpha, signed_vol, tot_vol; };
```

`price` is SIP's current log price for the instrument. `alpha` is the part of the SIP prediction coming from trade-flow features only. `signed_vol` and `tot_vol` are emitted only when a trade event flushes accumulated volume.

Conceptually, SIP is a tiny online microprice model. Its baseline is the current log midpoint; its prediction adds quote-imbalance, stale-trade-location, and trade-flow terms. It deliberately remains per-instrument: it does not know about other exchanges for the same coin, the broader market, positions, or order parameters.

### SIP State And Initialization

Important state:

- `MDPrev`: previous quote/event.
- `midp`: current log midpoint, computed as `log(bid + ask) - log(2.0)`.
- `finalP`: latest SIP output log price.
- `feat[16]`: feature vector.
- `Reg`: online time-series regression from features to future midpoint.
- `tot_signedV`, `tot_V`: accumulated signed and absolute trade volume.
- `bqty`, `aqty`: queue-size state used to infer stable bid/ask quantities across book updates.
- `latest_trade_time`, `quote_outdated`: trade/quote timing state.

Initialization requires a real quote (`MD.ask != 0`). It sets:

- `midp = finalP = log((bid + ask) / 2)`.
- regression start value to `finalP`.
- features to zero.
- previous quote and queue quantities from the quote.

### SIP Features

SIP has exactly 16 features. Features 0-7 are quote/microprice and stale-trade-location features. Features 8-15 are decaying trade-flow features.

On quote ticks:

- `bqty` and `aqty` are updated with max/continuation logic that tries to avoid treating queue depletion and quote movement as naive size changes.
- `mp = (bqty - aqty) / (bqty + aqty + 1e-12)`.
- `mp2 = (MD.bidq - MD.askq) / (MD.bidq + MD.askq + 1e-12)`.
- If the quote is not older than the latest trade, `quote_outdated` is cleared and:
  - `feat[0] = mp`.
  - `feat[1] = mp^3`.
  - `feat[2] = mp2`.
  - `feat[3] = mp2^3`.
  - `feat[4..7] = 0`.
- `midp` becomes the new log midpoint and `MDPrev` becomes the new quote.

On trade ticks:

- Trade notional proxy `v = MD.bidq * (multiply_by_P_for_trade ? MD.bid : 1)` is accumulated into `tot_signedV` and `tot_V`.
- If `MD.askq == 0`, SIP returns immediately after accumulation. This allows a stream of trade fragments to be batched.
- Otherwise accumulated signed/total volume is flushed into the return value and reset.
- `sgn = sign(signed_volume)`.
- `sgnsqrt = sgn * sqrt(abs(signed_volume))`.
- The trade-flow EWM buckets are incremented:
  - `feat[8..11] += sgnsqrt`.
  - `feat[12..15] += sgn`.
- If the trade exchange timestamp is newer than the previous quote timestamp, SIP checks whether trade price is outside the previous quote:
  - `above_ask = MD.bid > MDPrev.ask`.
  - `below_bid = MD.bid < MDPrev.bid`.
  - `quote_outdated |= above_ask || below_bid`.
- While `quote_outdated` is true, SIP sets:
  - `feat[4] = 1e5 * (log(trade_price) - midp)`.
  - `feat[5] = above_ask - below_bid`.
  - `feat[6] = feat[0] + feat[2] + feat[1] + feat[3]`.
  - `feat[7] = sgnsqrt`.
- If the trade is not newer than the previous quote timestamp, SIP returns without refitting/predicting.

After each non-returning quote or trade path, `feat[8..15]` are decayed by `inv_trade_qty_decays`. The four decay scales are duplicated across the signed-square-root and sign buckets. They are derived from regression timespan samples and `trade_qty_decay_mult`.

### SIP Regression And Output

SIP calls:

```cpp
Reg.operator()<false>(MD.time, feat, midp)
```

The target is the log midpoint. When the adapter updates, SIP also rescales selected covariance rows if feature 5's covariance is too small, then recomputes trade decay multipliers.

Output is:

- `finalP = midp + dot(Reg.reg.coef[0..15], feat[0..15])`.
- `alpha = dot(Reg.reg.coef[8..15], feat[8..15])`.

The alpha is deliberately not added a second time: it is already included in `finalP` because `finalP` uses all 16 features. The separate `alpha` is passed to FPC as an explanatory feature for cross-instrument fair pricing.

This means the word `alpha` has two meanings depending on stage. SIP alpha is a local feature contribution still inside the per-instrument price model. `TradeableDataCache.alphas` later is a sampled cross-instrument price-distance alpha used in reference-price construction. They are related only because FPC can use SIP alpha as one of its explanatory inputs.

### SIP Training/Production Params

SIP params are loaded from `params/SIP.bin`, or `params/new_SIP.bin` if newer. Each record is `[instrument_uuid, serialized SIP state]`; the uuid maps to an active slot at startup. The research writer currently creates SIP states with:

- target timespan: 30 seconds.
- regression span: 20,000 timespans.
- fit fraction: usually 0.005 in `Research/script2.py`.
- shrinkage: usually 0.02.
- trade quantity decay multiplier: usually 60 in `Research/script2.py`.
- `multiply_by_P_for_trade`: true for non-coin-margined futures, false for `fut_coin`.

The runtime will save updated SIP state into `params/new_SIP.bin` after a long enough run and clean shutdown.

## FPC: Per-Coin Fair Price Calculator

`NewFairPriceCalculator` lives in `Trader/FairPriceCalculator.hpp`. There is one FPC per active priced coin: `FairPriceManager::fpcalcs[NPricing]`.

FPC combines multiple active instrument streams for the same coin into one coin-level fair log price plus per-instrument log offsets. With current compile-time constants:

- `NPricing = 48` priced coins.
- `Nfpc = 16` possible active instrument slots per coin.
- `NTradeablePerPricedMax = 2` tradeable instruments per priced coin in the downstream tradeable cache loop.

FPC is the boundary where exchange-specific prices become a coin price. It has to solve three problems at once: combine venues with different reliability/volume weights, maintain stable offsets between instruments, and produce a fair value that is less noisy than any one stream but still reacts quickly to useful moves.

### FPC Inputs

For every SIP update, `FairPriceManager::Calculate` calls:

```cpp
double Pnew = fpc.update(MD.time, idx.index_within_coin(), p, alpha);
```

The FPC input is:

- event time.
- slot index within the coin.
- SIP log price `p`.
- SIP trade-flow alpha `alpha`.

The FPC does not consume raw bid/ask directly. By the time a tick reaches FPC, SIP has already converted raw L1/trade state into a log-price estimate and a local trade-flow feature.

### FPC State

Important state:

- `prices[Nfpc]`: latest SIP log price per slot.
- `pmean_weight[Nfpc]`: static mean weights loaded from FPC params.
- `live_pmean_weight`: pmean weights after removing stale slots.
- `multiplier_offsets`: coarse log offsets by integer multiples of `log(10)`, used to normalize instruments with different quote multipliers.
- `Pmean`: weighted live mean log price.
- `Pfair`: predicted fair coin log price.
- `Plo`, `Phi`: low/high coin-level fair envelopes.
- `offsets[2][Nfpc]`: two exponentially updated offset estimates.
- `deltas[2][Nfpc]`: deviations of each slot from offset-adjusted mean.
- `low_offsets`, `high_offsets`: per-slot min/max of the two offset estimates.
- `prev_tot_prices`: previous mean reference per slot for intra-tick-return features.
- `feat`: FPC regression feature vector.
- `Reg`: online time-series regression from FPC features to future `Pmean`.
- `price_ewm`: EWM of `Pfair` used for low/high envelope construction.

### FPC Mean And Offsets

`set_prices_from_self()` seeds FPC from the current SIP prices:

1. Finds the minimum nonzero slot price.
2. Sets `multiplier_offsets[i] = log(10) * round((prices[i] - minp) / log(10))`.
3. Computes live weighted mean:

```text
Pmean = dot(live_pmean_weight, prices) / live_weight_sum - live_multiplier_offset
```

4. Initializes both offset vectors to `prices[i] - Pmean`.
5. Initializes regression start value and price EWM from `Pmean`.

On every update:

1. `prices[idx] = SIP price`.
2. `sip_alpha_ptr[idx] = SIP alpha`.
3. `dt = min(30 ms, T - last_time)`.
4. Each offset vector is advanced by its delta:

```text
offsets[j] += offsetdecays[j] * dt * deltas[j]
```

with two production decay constants:

- `2.0 / 3e10`.
- `2.0 / 12e10`.

These are nanosecond-scaled and correspond roughly to 30-second and 120-second offset half-response scales.

5. `low_offsets = min(offsets[0], offsets[1])`; `high_offsets = max(offsets[0], offsets[1])`.
6. `Pmeanprev = get_Pmean()`.
7. The updated slot's `prev_tot_prices[idx]` is set to current `Pmean`.
8. For each offset decay, `deltas[j] = prices - offsets[j] - Pmean`.

### FPC Features

FPC feature length is `N * 3 + 16`, where `N` is the number of active slots for that coin. The current feature layout is:

- `feat[0..7]`: EWMs of the sign of the latest `Pmean` return.
- `feat[8..15]`: EWMs of the latest `Pmean` return.
- `feat[16 .. 16+N-1]`: slow offset deltas, currently `deltas[1]`.
- `feat[16+N .. 16+2N-1]`: intra-tick returns, `Pmean - prev_tot_prices[i]`.
- `feat[16+2N .. 16+3N-1]`: SIP trade-flow alpha per slot.

Only the first `N` slot entries are copied for `deltas[1]` on every tick. `intra_tick_ret_ptr` and `sip_alpha_ptr` point directly into the feature vector, so those features are updated in place.

The EWM decay vector is recomputed from the regression timespan:

```text
ewmdecays[0] = 1000 * min(1 / timespan_samples, 0.001)
ewmdecays[i+1] = ewmdecays[i] / 3
```

Thus the first 16 features are short-to-long signed momentum and return state at the coin-composite level.

### FPC Regression And Output

FPC calls:

```cpp
auto [pred, updated] = Reg(T, feat, Pmean);
Pfair = Pmean + pred;
```

Then it builds a low/high envelope:

```text
ewm = price_ewm(dt, Pfair)
latest_perinstr_delta = deltas[0][idx] if the slot has significant pmean weight, else 0
Plo = min(Pfair + min(0, latest_perinstr_delta + 2e-4), ewm)
Phi = max(Pfair + max(0, latest_perinstr_delta - 2e-4), ewm)
```

Per-slot log outputs are:

- `fairprice(i) = Pfair + offsets[1][i]`.
- `loprice(i) = Plo + low_offsets[i]`.
- `hiprice(i) = Phi + high_offsets[i]`.

`Plo`/`Phi` are not confidence intervals in a statistical reporting sense. They are side-specific fair anchors for execution: bid-side logic uses the low anchor, ask-side logic uses the high anchor. This makes quoting less eager when the most recent per-instrument move is one-sided or the fair estimate is still catching up.

The `FairPriceManager::Calculate` return value `Pnew` is `Pfair`, the coin-level fair log price.

### FPC Staleness

Pricer checks one active slot per loop for staleness. If an active slot has not ticked for 120 seconds, `FairPriceManager::SetStaleness` calls `NewFairPriceCalculator::freeze_slot`.

Freezing a slot:

- Snapshots the two slot offsets.
- Freezes three slot feature coordinates by ridge-inflating covariance to `1e20`.
- Sets live pmean weight to zero.
- Recomputes live mean parameters.

Restoring reverses the feature covariance rows, restores offsets, restores pmean weight, and recomputes mean state. This removes stale slots from `Pmean` without adding a hot-path per-tick feature mask.

### FPC Cache Writes

After `Pnew` is computed, `FairPriceManager::Calculate` computes:

```text
ret = Pnew - exchange(CoinDataCache.prices[coinidx], Pnew)
fastsd = TickSD[coinidx].update(MD.time, ret)
```

`exchange` both reads the previous coin log price and writes the new one. Therefore `ret` is the latest coin fair log return.

For normal, non-warmup ticks, `UpdateCachesWithEstimatations` writes:

- `ActiveDataCache.rawprices`: FPC slot SIP log prices.
- `ActiveDataCache.fairprices`: `fpc.offsets[1] + Pnew`.
- `ActiveDataCache.loprices`, `hiprices`: slot low/high log prices.
- For each tradeable mapped to the coin:
  - `TradeableDataCache.rawprices = exp(fpc.prices[index_within_coin])`.
  - `TradeableDataCache.fairprices = exp(active fair log price)`.
  - `TradeableDataCache.loprices = exp(active low log price)`.
  - `TradeableDataCache.hiprices = exp(active high log price)`.
  - `TradeableDataCache.fastsds = fastsd`.
- `CoinDataCache.fastsds[coin_idx] = fastsd`.

The per-tradeable fair/low/high prices are the direct price anchor for order references.

## TSPCA: Online Cross-Coin Factor Model

`OTSPCA` lives in `Trader/EMPC.hpp`. In production it is embedded as:

```cpp
OTSPCA<true, NPCs, NPricing> tspca;
```

with `NPCs = 12` and `NPricing = 48`. It runs on the vector of coin-level FPC fair log prices, `CoinDataCache.prices`.

TSPCA is updated only on sampled updates, not every tick. The active production sample interval is loaded from `params/Clint.bin`; the current file contains `100,000,000 ns = 100 ms`.

The reason for this stage is that many coin moves are not idiosyncratic. A BTC/ETH/market shock should not be treated the same way as a residual move isolated to one coin. TSPCA decomposes sampled coin fair-price returns into a small factor span plus residuals, and both parts feed later alpha/risk logic.

### TSPCA Inputs And Outputs

On each sample:

```cpp
const auto& residdiffs = tspca(CoinDataCache.prices);
```

TSPCA computes:

```text
diffs = current_coin_log_prices - previous_coin_log_prices
```

Then sequentially for each principal component `i`:

```text
pcret[i] = dot(projw[i], diffs)
diffs -= pcret[i] * E[i]
```

After all components, `diffs` is the residual return vector. Public outputs are:

- `pcret[NPCs]`: sampled factor/PC returns.
- `pcprices[NPCs]`: cumulative PC prices, incremented by `pcret`.
- `E[i][coin]`: current normalized loading for PC `i`.
- `projw[i][coin]`: GLS projection weights for PC `i`.
- `get_sd()[coin]`: de-leveraged latent residual SD per coin.
- returned `residdiffs[coin]`: post-factor residual return per coin.

The decomposition is sequential: PC 0 is removed first, PC 1 is fit to what remains, and so on. `E[i]` and `projw[i]` should therefore be read as an ordered factor basis, not as independent columns of a symmetric batch PCA.

### TSPCA Weighting And Fitting

TSPCA maintains `residsd`, `weightsd`, `ivar`, and leverage. The public residual SD is `weightsd`, not raw post-fit residual SD. The code's intended interpretation is:

- `residsd` tracks observed post-fit residual scale.
- `weightsd` is de-leveraged latent residual scale used for GLS weighting.
- `weightsd` excludes variance currently explained by the PCA factor span.

`set_E()` rebuilds normalized loadings and projection weights:

```text
E = newE / Enorm
normalize each E[i]
ivar = 1 / weightsd^2
projw[i] = E[i] * ivar / dot(E[i], E[i] * ivar)
leverage += E[i] * projw[i]
weightsd += 0.25 * (residsd / sqrt(max(1 - leverage, 0.02)) - weightsd)
```

When `fit=true`, each sample also updates:

- EWM residual diffs per component.
- EWM PC returns.
- `newE` and `Enorm` using covariance-style updates.

Every 128 samples, `set_E()` is called to refresh loadings and projection weights.

### TSPCA Cache Writes

`FairPriceManager::SampledUpdate()` calls:

```cpp
SampledUpdateCaches(sac(residdiffs, tspca), tspca.get_sd(), tspca.get_Es(), tspca.get_pcprices());
```

`SampledUpdateCaches` writes:

- `PCCache.pcprices = tspca.get_pcprices()`.
- `CoinDataCache.slowresidsds = tspca.get_sd()`.
- `CoinDataCache.pcEs[i] = E[i]`.
- For each tradeable:
  - `TradeableDataCache.slowresidsds[tradeable] = CoinDataCache.slowresidsds[coin]`.
  - `TradeableDataCache.pcEs[i][tradeable] = CoinDataCache.pcEs[i][coin]`.

These `pcEs` are later used to convert PC-level alphas and risk skews onto tradeable instruments.

## SAC: Sampled Cross-Coin Alphas

`SampledAlphaCalculator` lives in `Trader/SampledAlphaCalculator.hpp`. It receives TSPCA residuals and TSPCA state and produces sampled alphas at the 100 ms sample cadence.

SAC is the production cross-instrument alpha layer. It uses the factor/residual representation from TSPCA so that market continuation/reversion, coin residual continuation/reversion, and broader all-to-all predictive structure can be controlled separately.

Production sampled alpha names are defined in `Core/KeyDataTypes.hpp`:

- PC sampled alpha: `mkt_mktalpha`.
- Coin sampled alphas: `resid_residalpha`, `all_allalpha`.

The production alpha taxonomy is:

- `mkt_mktalpha`: sampled first-PC/market alpha.
- `resid_residalpha`: sampled per-coin residual alpha.
- `all_allalpha`: sampled all-factor/all-residual regression alpha reconstructed per coin.
- `ultra_fast_mkt_mktalpha`: intra-sample first-PC/market alpha.
- `ultra_fast_resid_residalpha`: intra-sample per-coin residual alpha.

There is no current production path populating sampled `pc_residalpha` or `resid_mktalpha`; older names appear only in comments or analysis code.

SAC returns a tuple:

```cpp
make_fast_tuple(
    ewm_norm_mkt(norm_mkt_ret),
    resid_residalpha_ewm(norm_residdiffs) * resid_residalpha_coef * rsd2,
    f_all_all_alpha_from_reg
)
```

`SampledUpdateCaches` destructures that tuple as `SAMPLEDALPHAS` and assigns:

- `PCCache.mkt_mktalpha[0] = mkt_mktalpha`.
- `CoinDataCache.resid_residalpha = resid_residalpha`.
- `CoinDataCache.all_allalpha = all_allalpha`.

Only `PCCache.mkt_mktalpha[0]` is assigned; other PC alpha slots are not populated by this path.

### `mkt_mktalpha`

SAC computes:

```text
s = ews_mkt(pcret[0])
norm_mkt_ret = pcret[0] / (s * s)
mkt_mktalpha = ewm_norm_mkt(norm_mkt_ret)
```

This is a scalar market-on-market alpha: a normalized first-PC return is smoothed by a configured EWM and scaled by its coefficient. It is later mapped to tradeables through each tradeable's first PC exposure.

In `SampledRecalculate`:

```text
tradeable sampled market alpha =
    TDC.pcEs[0][i] * PC.mkt_mktalpha_mult[i] * PCCache.mkt_mktalpha[0] * TDC.fairprices[i]
```

The multiplication by fair price converts the log-return-like alpha to a price-distance alpha.

### `resid_residalpha`

SAC first obtains residual scales:

```text
residsd = tspca.get_sd()
rsd2 = residsd2(residdiffs)
norm_residdiffs_for_reg = residdiffs / residsd
...
norm_residdiffs_for_resid_alpha = residdiffs / rsd2
```

The sampled residual-on-residual alpha is:

```text
resid_residalpha = resid_residalpha_ewm(residdiffs / rsd2) * resid_residalpha_coef * rsd2
```

This is per coin. It is a smoothed normalized residual return, coefficient-scaled, then rescaled by residual volatility. In tradeable alpha construction:

```text
tradeable residual alpha =
    PC.resid_residalpha_mult[i] * CoinDataCache.resid_residalpha[coin] * TDC.fairprices[i]
```

### `all_allalpha`

SAC builds one combined feature vector:

```text
all_stuff[0:NPCs] = pcret / pc_ews(pcret)
all_stuff[NPCs:NPCs+NPricing] = residdiffs / tspca.get_sd()
```

Then it runs an online multi-output regression:

```cpp
pred = reg_f_all_all.fit_ewm_predict(all_stuff, all_stuff);
```

The prediction is mapped back into coin residual/price space:

```text
f_all_all_alpha_from_reg = pred_coin_slice * residsd
for each PC i:
    f_all_all_alpha_from_reg += (pcsd[i] * pred[i]) * E[i]
```

This is the broadest sampled cross-instrument alpha in production. It lets normalized PC and residual state predict the whole normalized PC+residual vector, then reconstructs a per-coin alpha from both predicted residual moves and predicted factor moves.

In tradeable alpha construction:

```text
tradeable all-all alpha =
    PC.all_allalpha_mult[i] * CoinDataCache.all_allalpha[coin] * TDC.fairprices[i]
```

### Sampled Alpha Cache-To-Tradeable Combination

At sampled recalculation time:

```text
TDC.alphas =
    TDC.pcEs[0] * PC.mkt_mktalpha_mult * PCCache.mkt_mktalpha[0]

for each tradeable i:
    coin = IM.tradeable_to_coinidx[i]
    TDC.alphas[i] +=
        PC.all_allalpha_mult[i] * CoinDataCache.all_allalpha[coin]
      + PC.resid_residalpha_mult[i] * CoinDataCache.resid_residalpha[coin]

TDC.alphas *= TDC.fairprices
```

Thus `TradeableDataCache.alphas` is a sampled price-distance alpha. It includes:

- first-PC market-on-market alpha, exposure-mapped by `pcEs[0]`;
- per-coin all-all cross alpha;
- per-coin residual-residual alpha.

The current `params/Clint.bin` has all 59 serialized tradeable slots nonzero for all five production alpha multipliers:

| Multiplier | Current value on nonzero slots |
| --- | ---: |
| `mkt_mktalpha_mult` | 0.5 |
| `resid_residalpha_mult` | 0.5 |
| `all_allalpha_mult` | 0.6 |
| `ultra_fast_mkt_mktalpha_mult` | 0.5 |
| `ultra_fast_resid_residalpha_mult` | 0.5 |

The instrument file currently in the workspace has `Tradeable = 0` for all rows, while `Clint.bin` still contains 59 tradeable parameter slots. The algorithmic statement above follows the runtime code and serialized Clint parameter file; live deployment should be checked against the actual instrument map used by the running process.

## Ultra-Fast Intra-Sample Alphas

Sampled alphas refresh only at the sample interval. To avoid losing information between samples, SAC also maintains intra-sample state updated on every coin fair-price tick.

This is the second alpha time scale. Sampled alphas are refit/recomputed at the 100 ms grid; ultra-fast alphas are updated as coin fair prices move inside that grid. They are not written into `TradeableDataCache.alphas`; instead they are added directly to the low/high price anchors in `RecalculateOrderRefPrices`.

After every FPC update, `FairPriceManager::Calculate` calls:

```cpp
sac.intra_sample_update(coinidx, ret, tspca);
```

where `ret` is the latest coin fair log return.

### Intra-Sample Betas

At sampled update time, SAC calls `calculate_intra_sample_betas(tspca)` to prepare maps from a coin tick return into:

- PC return increments: `coin_to_Es[coin_idx]`.
- ultra-fast residual alpha increments: `coin_to_uf_residalpha_betas[coin_idx]`.

The first PC is special:

```cpp
tspca.factor_projw(E[0] * uf_mkt_E_adjustment, uf_Es[0]);
```

That means the ultra-fast market estimator can use an adjusted first-PC exposure vector rather than raw `projw[0]`. For PCs 1..`NPCs-1`, SAC uses `tspca.get_projw()[i]`.

Then SAC constructs triangular/sequential projection coefficients so a single coin return is decomposed consistently with the sequential TSPCA residualization. The resulting `coin_to_uf_residalpha_betas` maps a tick return into residual-alpha space, scaled by `uf_resid_resid_coef`.

### Tick Updates And Decay

On every coin fair-price return:

```text
uf_pcret += ret * coin_to_Es[coin_idx]
ewm_intra_sample_mkt_on_mkt.value += ret * uf_Es[0][coin_idx]
uf_resid_residalpha += ret * coin_to_uf_residalpha_betas[coin_idx]
```

Every millisecond, `ClintType::SampledUpdate` calls `sac.intra_sample_decay(time)`, which:

- decays `ewm_intra_sample_mkt_on_mkt` by its configured EWM decay and elapsed nanoseconds, capped at 100 ms;
- decays `uf_resid_residalpha` by `1 - uf_resid_resid_decay * delta_T`.

### Finalization Into Order Reference Prices

`ClintType::RecalculateOrderRefPrices` calls:

```cpp
auto&& [uf_pcret, uf_mkt_mktalpha, uf_resid_residalpha] =
    fpm.sac.finalize_intra_sample_update(fpm.tspca);
```

Finalization:

- returns accumulated `uf_pcret` and resets it to zero;
- returns `ewm_intra_sample_mkt_on_mkt.get_pred()`;
- returns the current `uf_resid_residalpha`.

Then:

```text
PCCache.pcprices += uf_pcret
PCCache.ultra_fast_mkt_mktalpha = uf_mkt_mktalpha
CoinDataCache.ultra_fast_resid_residalpha = uf_resid_residalpha
```

The ultra-fast alphas used in order reference prices are:

```text
uf_mkt_mktalpha_factors =
    PC.ultra_fast_mkt_mktalpha_mult * TDC.pcEs[0] * TDC.fairprices

uf_resid_residalpha_factors =
    PC.ultra_fast_resid_residalpha_mult * TDC.fairprices

intra_sample_alphas[i] =
    uf_mkt_mktalpha_factors[i] * PCCache.ultra_fast_mkt_mktalpha
  + uf_resid_residalpha_factors[i] * CoinDataCache.ultra_fast_resid_residalpha[coin]
```

So ultra-fast market alpha is scalar and exposure-mapped by first-PC `E`; ultra-fast residual alpha is per coin and then copied to tradeables by coin mapping. Both are converted to price units by fair price.

`PCCache.pcprices += uf_pcret` means consumers see PC prices with intra-sample PC moves included between sampled TSPCA updates.

## Risk Skews

Before reference widths are calculated, `SampledRecalculate` calls:

```cpp
fpm.risky_mou.CalculateSkews();
```

`Riskooor::CalculateSkews` sets:

```text
TDC.skews =
    (TDC.targetposes - TDC.poses * TDC.fairprices)
  * ParamCache.PosRectCancel
  * TDC.slowresidsds
```

Then for each active account it adds PC/account-level skew:

```text
pcrect =
    -AccountCache.PCSkewMultiplier
  * dot(account_factors_per_instrument, ParamCache.PosRectCancel)
  / number_enabled_instruments_for_account

skewcontrib = dot(account_pcrects, PCCache.pcsds) * pcrect
TDC.skews += account_factors_per_instrument * skewcontrib
```

Finally:

```text
TDC.skews *= TDC.fairprices
```

Skews are price-distance adjustments. They are not alphas; they are inventory/risk pressure terms that enter cancel/retreat/add reference prices with `ApproachSkewMultiplier` and related rectification parameters.

The sign convention follows the later width formulas rather than a standalone "buy/sell signal" interpretation. A positive skew pushes the ask side wider/higher and affects inter-order spacing; a negative skew analogously affects the bid side. The source is current position versus target position, scaled by residual risk and account-level PC risk pressure.

## Sampled Recalculation: Widths, Conditions, Quantities

`ClintType::SampledUpdate(time)` performs sampled work when `time >= next_sample_time`:

1. Marks `AllCaches.ready = false`.
2. Advances `next_sample_time` to the next multiple of `sample_freq`.
3. Runs TSPCA/SAC sampled update through `fpm.SampledUpdate()`.
4. Runs `SampledRecalculate(time)`.
5. Marks `AllCaches.ready = true` with release memory ordering.
6. Every 10 seconds stores a fair-price history snapshot.

`SampledRecalculate` is where sampled alphas, risk skews, widths, quantities, and conditions are prepared.

This function is deliberately heavier than the per-tick path. It does vector-wide work across all tradeables: sampled alphas, risk skews, min edges, order spacing, quote sizes, and conditions. The per-tick path then reuses these precomputed widths and only adds ultra-fast alpha plus current low/high anchors.

### Baseline Alpha And Skew Price

After sampled `TDC.alphas` and `TDC.skews` are computed:

```text
alpha_baseline_skew_price = TDC.alphas + PC.ApproachSkewMultiplier * TDC.skews
```

This is the central directional price offset for cancel and retreat bands.

### Cancel Widths

```text
base_cancel = (PC.CancelBps + PC.CancelWidth * TDC.fastsds) * TDC.fairprices

TDC.bid_cancel_widths = alpha_baseline_skew_price - base_cancel
TDC.ask_cancel_widths = alpha_baseline_skew_price + base_cancel

min_cancel_edge =
    (PC.MinCancelEdgeBps + PC.MinCancelEdgeWidth * TDC.fastsds)
  * TDC.fairprices

TDC.bid_cancel_widths = min(TDC.bid_cancel_widths, -min_cancel_edge)
TDC.ask_cancel_widths = max(TDC.ask_cancel_widths,  min_cancel_edge)
```

Bid cancel widths are forced nonpositive by min edge; ask cancel widths are forced nonnegative.

### Retreat Widths

The code reuses `thingy` after cancel base and adds retreat base:

```text
base_cancel_plus_retreat =
    base_cancel
  + (PC.RetreatBps + PC.RetreatWidth * TDC.fastsds) * TDC.fairprices

TDC.bid_retreat_widths = alpha_baseline_skew_price - base_cancel_plus_retreat
TDC.ask_retreat_widths = alpha_baseline_skew_price + base_cancel_plus_retreat
```

Then it applies the non-approach part of skew only on the adverse side:

```text
extra_skew = (1 - PC.ApproachSkewMultiplier) * TDC.skews
TDC.bid_retreat_widths += (TDC.skews < 0) * extra_skew
TDC.ask_retreat_widths += (TDC.skews > 0) * extra_skew
```

And clamps by min retreat edge:

```text
min_retreat_edge =
    (PC.MinRetreatEdgeBps + PC.MinRetreatEdgeWidth * TDC.fastsds)
  * TDC.fairprices

TDC.bid_retreat_widths = min(TDC.bid_retreat_widths, -min_retreat_edge)
TDC.ask_retreat_widths = max(TDC.ask_retreat_widths,  min_retreat_edge)
```

### Inter-Order, BBO, Aggression, And Quantity Fields

Inter-order spacing:

```text
aws = (PC.InterOrderWidth * TDC.fastsds + PC.InterOrderBps) * TDC.fairprices
InterOrderSkews = PC.InterOrderRectMultiplier * TDC.skews

TDC.bid_add_widths = max(aws * PC.MinInterOrderRectFraction, aws - InterOrderSkews)
TDC.ask_add_widths = max(aws * PC.MinInterOrderRectFraction, aws + InterOrderSkews)

TDC.bid_add_widths_ts = max(TDC.bid_add_widths, TDC.TS_minuseps)
TDC.ask_add_widths_ts = max(TDC.ask_add_widths, TDC.TS_minuseps)
```

BBO exclusion and aggression widths:

```text
TDC.bbo_widths =
    (PC.BBOExclusionBps + PC.BBOExclusionWidth * TDC.fastsds)
  * TDC.fairprices

TDC.initiation_aggress_widths =
    (PC.InitiationAggressWidth * TDC.fastsds + PC.InitiationAggressBps)
  * TDC.fairprices

TDC.aggress_widths =
    (PC.AggressWidth * TDC.fastsds + PC.AggressBps)
  * TDC.fairprices
```

Quote sizes:

```text
q_jitter = rnd() % 128
TDC.Qs = round_qty((PC.OrderSize + q_jitter) / TDC.fairprices)
maxposes = min(PC.MaxPosition, PC.MaxRisk / TDC.slowresidsds) / TDC.fairprices
maxposes_95 = 0.95 * maxposes

TDC.buy_Qs =
    min(TDC.Qs, round_qty(maxposes - TDC.poses))
    if maxposes_95 > TDC.poses else 0

TDC.sell_Qs =
    max(-TDC.Qs, -round_qty(maxposes + TDC.poses))
    if maxposes_95 > -TDC.poses else 0
```

Current buy/sell conditions do not check alpha thresholds; they are only position/quantity gates:

```text
TDC.buy_cond[i] = TDC.buy_Qs[i] > 0
TDC.sell_cond[i] = TDC.sell_Qs[i] < 0
```

`bbo_cond` is an EWM/hysteresis condition:

```text
TDC.bbo_cond[i] = TDC.minimumspreadfrac[i] >= 0.4 - 0.1 * TDC.bbo_cond[i]
```

## Order Reference Prices

`RecalculateOrderRefPrices` runs after every fresh input batch and at the end of sampled recalculation. This function is the last stage of the pricing path before order code.

This is the practical output of the pricing system. Everything upstream exists so these fields can be updated cheaply and read by `OrderCalculator` without re-running pricing logic or factor models.

It first finalizes ultra-fast alpha state and builds:

```text
lo_alpha_P = TDC.loprices + intra_sample_alphas
hi_alpha_P = TDC.hiprices + intra_sample_alphas
```

Then order references are:

```text
TDC.bid_cancel_Ps  = lo_alpha_P + TDC.bid_cancel_widths
TDC.ask_cancel_Ps  = hi_alpha_P + TDC.ask_cancel_widths

TDC.bid_retreat_Ps = lo_alpha_P + TDC.bid_retreat_widths
TDC.ask_retreat_Ps = hi_alpha_P + TDC.ask_retreat_widths

TDC.bid_Ps = min(TDC.bid_retreat_Ps - TDC.bid_add_widths, TDC.midprices)
TDC.ask_Ps = max(TDC.ask_retreat_Ps + TDC.ask_add_widths, TDC.midprices)

TDC.bid_approach_Ps = TDC.bid_Ps - TDC.bid_add_widths_ts
TDC.ask_approach_Ps = TDC.ask_Ps + TDC.ask_add_widths_ts

TDC.buy_agg_Ps  = TDC.bid_retreat_Ps - TDC.aggress_widths
TDC.sell_agg_Ps = TDC.ask_retreat_Ps + TDC.aggress_widths
```

Interpretation:

- Low/high fair prices, not midpoint fair price, anchor the two sides.
- Sampled alphas affect the precomputed widths through `TDC.alphas`.
- Ultra-fast alphas are added directly to low/high anchors every reference-price recalculation.
- Skews affect sampled widths and inter-order spacing, not the low/high anchors directly.
- `bid_Ps` and `ask_Ps` are bounded by current market midpoint so passive quoting does not cross the midpoint through this path.
- Cancel and retreat references are intentionally separate: cancel prices are nearer thresholds for invalidating existing orders; retreat prices are farther references used for moving orders away and building quote targets.

`Trader/OrderCalculator.hpp` consumes these fields. It uses:

- cancel prices to decide cancellation urgency;
- retreat prices to move or cancel stale/aggressive orders;
- bid/ask prices as target passive placement levels;
- approach prices for moving orders closer;
- aggression prices for initiating or maintaining aggressive orders;
- add widths for spacing multiple orders.

## Cache Outputs Published By Pricer

The main shared-memory outputs written by this path are:

Cache naming is terse because these fields are hot-path shared-memory structures. The broad split is: active-slot log diagnostics in `ActiveDataCache`, coin/factor state in `CoinDataCache` and `PCCache`, and executable tradeable state in `TradeableDataCache`.

### `ActiveDataCache`

Active-slot, log-space diagnostics:

- `last_tick_time`.
- `bids`, `asks`.
- `rawprices`: SIP log prices per active slot.
- `fairprices`: FPC fair log prices per active slot.
- `loprices`, `hiprices`: FPC low/high log prices per active slot.

### `CoinDataCache`

Coin-level log/factor/alpha state:

- `prices`: FPC coin fair log prices.
- `fastsds`: tick-level fair-return SD from `TickSD`.
- `slowresidsds`: TSPCA residual SD (`weightsd`).
- `pcEs[NPCs]`: current TSPCA loading vector per PC.
- `resid_residalpha`: sampled residual alpha per coin.
- `all_allalpha`: sampled all-all alpha per coin.
- `ultra_fast_resid_residalpha`: intra-sample residual alpha per coin.

### `PCCache`

PC-level state:

- `pcprices`: sampled PC prices plus intra-sample increments.
- `pcsds`: PC SDs used by risk skew code.
- `mkt_mktalpha[0]`: sampled market-on-market alpha.
- `ultra_fast_mkt_mktalpha`: intra-sample market-on-market alpha.

### `TradeableDataCache`

Tradeable, normal-price outputs:

- Market: `bids`, `asks`, `midprices`, `minimumspreadfrac`.
- Prices: `rawprices`, `fairprices`, `loprices`, `hiprices`.
- Vols/risks: `fastsds`, `slowresidsds`, `pcEs`.
- Position/risk: `poses`, `targetposes`, `skews`.
- Alpha: `alphas`, the sampled price-distance alpha.
- Widths: cancel, retreat, add, BBO, initiation-aggress, aggress.
- Reference prices: `bid_cancel_Ps`, `ask_cancel_Ps`, `bid_retreat_Ps`, `ask_retreat_Ps`, `bid_Ps`, `ask_Ps`, `bid_approach_Ps`, `ask_approach_Ps`, `buy_agg_Ps`, `sell_agg_Ps`.
- Quantities/conditions: `Qs`, `buy_Qs`, `sell_Qs`, `buy_cond`, `sell_cond`, `bbo_cond`.

These are published by `Cache_shm.publish()` in Pricer after each fresh input batch.

`TradeableDataCache` is the main contract with trading executables. If a downstream component only needs executable levels, it should not need to understand SIP/FPC/TSPCA internals; it can read the reference prices, quantities, conditions, and current market/fair fields from this cache.

## Order-Update Enrichment

Although not part of price formation, order updates and synthetic trade fills are enriched with pricing-path state by `EnrichOrderUpdate`.

For a tradeable order update it attaches:

- raw/fair/low/high tradeable price;
- fast SD;
- PC prices;
- coin log price;
- all coin alphas (`ultra_fast_resid_residalpha`, `resid_residalpha`, `all_allalpha`);
- sampled PC alpha projected to the tradeable (`mkt_mktalpha`);
- ultra-fast market alpha projected to the tradeable;
- current position.

This is why downstream DB/trade analysis can attribute fills to the alpha state present when the order update was produced.

## Parameter Files Used In Production

`ClintType` and `FairPriceManager` load all pricing parameters from `params/` at startup:

- `FPC.bin` or newer `new_FPC.bin`: serialized `NewFairPriceCalculator` state for every active priced coin.
- `SIP.bin` or newer `new_SIP.bin`: serialized `SIP` state for every active instrument.
- `MPC.bin` or newer `new_MPC.bin`: serialized TSPCA and SAC state.
- `Clint.bin`: first 8 bytes interpreted as `uint64_t sample_freq`, followed by a dense matrix of tradeable params.

The pricing process is therefore mostly stateful binary-model code at runtime. The C++ code defines model structure and update equations; the files in `params/` define the fitted coefficients, regression state, TSPCA state, SAC coefficients/decays, per-coin FPC weights/state, per-instrument SIP state, and per-tradeable execution multipliers.

`Clint.bin` is laid out by parameter first, tradeable second:

```text
[sample_freq_uint64_bits_as_first_8_bytes]
for each TradeableParam:
    for each tradeable:
        value
```

The current workspace `Clint.bin` contains:

- `sample_freq = 100,000,000 ns`.
- 32 tradeable params.
- 59 serialized tradeable slots.
- nonzero production alpha multipliers listed above.

Runtime can save updated FPC/SIP/MPC state as `new_FPC.bin`, `new_SIP.bin`, and `new_MPC.bin` after at least 12 hours of Pricer runtime and clean SIGINT shutdown. `Clint.bin` tradeable params are not saved by Pricer.

Startup chooses the newer of each old/new model file pair. That means a previous long-running production process can leave `new_FPC.bin`, `new_SIP.bin`, or `new_MPC.bin` that silently supersedes the base file.

## Offline Fitting Path Relevant To Production

The current research fitting script is `Research/script2.py`, with nanobind helpers in `Research/nb/nbm.cpp` and writers in `Research/misc.py`.

This section is included only to explain where the binary runtime state comes from. Production Pricer does not import this Python code. It only reads the emitted binary files.

The high-level fitting flow is:

1. Load compressed historical tick data for active instruments from `~/ProdData3/<date>/<exchange>/<type>/<id>.zstd`.
2. For each active instrument, run `RunSIP` over historical market-data rows to fit/update SIP state and emit SIP prices, signed volume, total volume, and SIP alpha.
3. For each coin, combine instrument streams with `CombinerRunner` at the configured `sample_interval` (currently 100 ms in research defaults).
4. Build pmean weights from adjusted average daily USD volume:
   - Hyperliquid volume multiplied by 0.1.
   - Gateio volume multiplied by 0.33.
   - Binance volume multiplied by 2.0.
   - Spot volume multiplied by 3.0.
   - Then square-root and normalize.
5. Create FPC params with `SetupFPCParams`.
6. Run `ResamplingPricer` to generate coin fair-price arrays.
7. `do_mpc()` builds a dataframe of fair prices, signed volumes, and total volumes for MPC/TSPCA/SAC fitting and writes `~/prices3.mmap`.

`Research/misc.py` contains writers for production binary params:

- `write_all_sips()`.
- `write_fpc_params()`.
- `write_clint_params()`.
- `write_mpc_params()` / `MPCWriter`.

The runtime report above is independent of the fitting implementation: whatever offline process writes the binary params, production consumes them through the serialized `serde` layout and executes the path described here.

Because serialization is positional, code changes to model fields or enum parameter order are production-breaking unless the corresponding binary writer and parameter files are regenerated in lockstep.

## End-To-End Formula Summary

Per active instrument tick:

```text
SIP log price:
    p_inst = log_mid + dot(beta_sip, sip_features)

SIP trade alpha:
    alpha_sip = dot(beta_sip_trade_features, sip_trade_features)

FPC coin fair:
    Pmean = weighted_mean(latest p_inst adjusted by multiplier offsets and live weights)
    Pfair = Pmean + dot(beta_fpc, fpc_features)

Per-tradeable fair anchors:
    fairprice = exp(Pfair + offset_slow)
    loprice   = exp(Plo   + offset_low)
    hiprice   = exp(Phi   + offset_high)
```

At each sample:

```text
coin_returns = current_coin_log_fairs - previous_coin_log_fairs
pcret, residual_returns = sequential_GLS_TSPCA(coin_returns)

mkt_mktalpha      = EWM(pcret[0] / market_scale^2) * coef
resid_residalpha  = MultiEWM(residual_returns / resid_scale) * coef * resid_scale
all_allalpha      = reconstruct_coin_alpha(online_reg([normalized PCs, normalized residuals]))
```

Sampled tradeable alpha:

```text
sampled_alpha_price[i] =
    fairprice[i] * (
        pcE0[i] * mkt_mktalpha_mult[i] * mkt_mktalpha
      + all_allalpha_mult[i] * all_allalpha[coin(i)]
      + resid_residalpha_mult[i] * resid_residalpha[coin(i)]
    )
```

Between samples:

```text
uf_mkt_mktalpha, uf_resid_residalpha = intra_sample_tick_accumulators(coin fair returns)

intra_sample_alpha_price[i] =
    fairprice[i] * (
        ultra_fast_mkt_mktalpha_mult[i] * pcE0[i] * uf_mkt_mktalpha
      + ultra_fast_resid_residalpha_mult[i] * uf_resid_residalpha[coin(i)]
    )
```

Reference prices:

```text
lo_alpha = loprice + intra_sample_alpha_price
hi_alpha = hiprice + intra_sample_alpha_price

bid_cancel  = lo_alpha + bid_cancel_width
ask_cancel  = hi_alpha + ask_cancel_width
bid_retreat = lo_alpha + bid_retreat_width
ask_retreat = hi_alpha + ask_retreat_width
bid_quote   = min(bid_retreat - bid_add_width, midprice)
ask_quote   = max(ask_retreat + ask_add_width, midprice)
```

The pricing path therefore has three nested time scales:

- tick scale: SIP, FPC, fast SD, ultra-fast alpha accumulation, reference-price refresh;
- millisecond scale: intra-sample alpha decay;
- sample scale, currently 100 ms: TSPCA, sampled SAC alphas, risk/skew/width/quantity recomputation.



---

I've noticed we sometimes get picked off when e.g. BTC or ETH spikes and we don't react fast enough on other instruments. I want to add some cheap, principled mitigation for this. In theory this is what uf_resid_residalpha is for, and while it has helped, it doesn't always seem to catch the full extent of the issue. 

One idea I had was that on shorter timescales, the residual volatility of BTC would be lower than other less efficient coins, and therefore for shorter-term TSPCA estimation, it should naturally get more weight. That would consequently lead to strong catch up alphas via uf_resid_residalpha on other coins. However the starting theory about residual volatility didn't seem to play out. 

We could of course just bump the residual volatility of coins inversely to their liquidity or something, but it's a bit hacky.

We could also be more conservative - we can start propagating hi/lo prices across instruments (i.e. we use BTC hi/lo and not just fair as pricing input). We can also do a more conservative version of uf_resid_residalpha where we generate another kind of hi/lo by saying e.g. if BTC moved X then coin Y could move by up to beta_Y/beta_BTC * X. These are likely to widen our pricing quite a bit though and feel hacky and icky. 

What ideas do you propose? Let me know also if any more info would be helpful.

To give you a little more context on something: 