# TrafficSense — Analytical Questions (summary)

Each question has its own report with method, computed tables (CSV), figures, interpretation and limitations. Data: cleaned PEMS-BAY readings, 325 sensors, 2017-01-01 → 2017-06-30 (local time).

## [Q1 — Daily and weekly congestion patterns](q1.md)

_When during the day and the week does congestion occur, how strong are the peaks, and how do weekends and holidays differ from working days?_

- Working-day **PM peak at 17:00** (25.3% of readings congested) is 1.4× the **AM peak at 08:00** (17.6%); midday 0.5%, night 0.04%.
- Working days average 5.51% congested readings vs 0.30% on weekends (18.6×). The 4 federal holidays average 1.03%, much closer to weekends than to working days (3.5× the weekend level).
- Most congested weekday: **Thu** (6.14%, 477 congested sensor-hours/day); lightest: Fri (4.28%).
- A typical working day accumulates 428 congested sensor-hours (worst day 2017-01-18, Wed: 650).

## [Q2 — Consistency of recurring traffic profiles](q2.md)

_Is congestion recurrent (same place, same time, most working days) or sporadic, and how consistent are each sensor's daily speed profiles from day to day?_

- **69.3%** of all congested sensor-slot-days on working days are **recurrent** (the slot is congested on ≥ 50% of working days); the remaining 30.7% are non-recurrent (incidents, special events, demand spikes). Total: 246,448 congested sensor-slot-days.
- Of 210 congestion-prone sensors (≥ 2% of working-day readings congested), 109 are highly recurrent (≥ 70% of their congestion is recurrent) and 42 are mostly sporadic (< 30%).
- Daily speed profiles of congestion-prone sensors closely follow their average profile: median day-vs-average correlation 0.96, median RMSE 4.1 mph.
- PM congestion starts at a consistent time: median day-to-day standard deviation of the onset is 38 minutes (sensor 407342 is the most consistent profile, r = 0.99).

## [Q3 — Congestion episode duration and recovery](q3.md)

_How long do congestion episodes last, when do the long ones happen, and how quickly does traffic recover to free flow after an episode ends?_

- 52,017 episodes on 319 sensors. The median lasts **30 min** (p90 185 min, longest 13.4 h), and 70.1% last ≥ 15 min (the Caltrans duration criterion).
- Duration is heavy-tailed: episodes ≥ 1 h are 34.4% of episodes but **80.5% of all congested minutes**.
- AM peak episodes last longer (median 35 min, 37.0% ≥ 1 h) than PM peak episodes (median 25 min, 33.0% ≥ 1 h); PM peak has 23,470 episodes vs 19,533 (more, shorter episodes).
- Recovery to free flow (≥ 60 mph) takes a median **50 min** after the episode ends (p90 145 min); 17.2% of episodes do not reach free flow within 3 h.

## [Q4 — Temporal speed volatility](q4.md)

_When and where are speeds most volatile from one 5-minute interval to the next, and is volatility linked to congestion?_

- Across 16,880,017 consecutive observed pairs, the 5-min speed change has std **1.75 mph**; 0.48% are sudden (|Δ| ≥ 10 mph).
- On working days volatility peaks at **18:00** (std Δ 2.46 mph), 1.6× the 01–04h level (1.54 mph). Weekends peak at 11:00 with 1.64 mph.
- Sudden changes are 3.7× more frequent in the working-day PM peak (0.96%) than at night (0.26%).
- Per sensor, volatility rises with congestion: Pearson r = 0.44 (Spearman 0.67) between std Δ and congestion rate over 325 sensors. The most volatile sensor is 401351 (std Δ 6.15 mph, 1.1% congested).

## [Q5 — Persistent congestion hotspots](q5.md)

_Which sensors are congested persistently (most working days, for substantial time), are the hotspots stable month to month, and do they cluster along the road network?_

- **148 persistent hotspots** (45.5% of sensors) have ≥ 60 congested minutes on at least half of working days. A further 56 sensors reach that on 20–50% of working days, and 28 never do (sensitivity: 179 hotspots at ≥ 30 min, 148 hotspots at ≥ 60 min, 97 hotspots at ≥ 120 min).
- Congestion is concentrated: the top 10 sensors (3.1% of sensors) account for 11.8% of all working-day congested minutes. The #1 sensor 400586 averages 365 congested min/day (persistence 100.0%).
- Hotspots are stable: month-to-month Spearman rank correlation 0.94–0.97, top-20 Jaccard overlap 0.65 on average; 10 sensors are in the top 20 in every month.
- Spatial clustering is weak: 59.9% of a hotspot's graph neighbours are hotspots vs 45.4% at random (1.3× lift).

## [Q6 — Congestion by road characteristics](q6.md)

_Do sensors with different road characteristics (design speed, network density, merge/diverge position, corridor) experience systematically different congestion?_

- **None of the derived characteristics explains much of the congestion differences**: the largest η² is 0.021 (Free-flow speed band), a small effect, and all four lie between 0.016 and 0.021. Congestion is site-specific rather than determined by these coarse attributes.
- Free-flow speed band: 12.5% PM-peak congestion in the lowest band vs 16.5% in the highest; Spearman ρ = -0.03, i.e. no meaningful monotonic relation.
- Network density: Spearman ρ = -0.02 between graph degree and PM-peak congestion (no meaningful relation); the highest graph-position group is 'diverge-like (out > in)'.
- KMeans found 3 corridors (silhouette 0.61); most congested: C3 NW (9 km), least congested: C2 S (7 km) (η² 0.016).

## [Q7 — Congestion propagation (lagged associations)](q7.md)

_Does congestion travel along the road network, in which direction (with or against the traffic flow), and with what time lag?_

- Neighbouring sensors move together: median correlation of same-time speed anomalies 0.27 over 2,369 directed edges; 1,147 edges (48.4%) have a peak correlation ≥ 0.3.
- Direction at 5-minute resolution: **no dominant direction**. On 42.2% of the informative edges the downstream sensor leads (asymmetry > 0); 53.6% peak at lag 0 (median peak lag +0 min, IQR +0 to +5). Linked sensors are a median 2.2 km apart (straight line). Vehicles cover that in about 1 min at 60 mph, and a backward-moving queue front (typically ~15–20 km/h) in about 7–9 min, at or below the 5-minute resolution, so the direction is hard to resolve.
- Event view: 15 min after a downstream onset, the upstream neighbour is congested with probability 32.7% (3.7× its baseline 8.85%); after an upstream onset, the downstream neighbour is congested with probability 32.1% (3.9× baseline). The two directions are practically symmetric.
- Each sensor's own anomaly persists: median autocorrelation 0.95 at 5 min, 0.81 at 15 min, 0.63 at 30 min and 0.43 at 60 min. This memory is why the persistence baseline is strong at 5 minutes.

## [Q8 — Sensor performance relative to peers](q8.md)

_Which sensors are consistently slower or more congested than their neighbouring sensors (local bottlenecks), and which disagree with their peers in ways that suggest detector problems?_

- Most sensors track their peers closely: median correlation with peer speed **0.84**, median |residual| 3.4 mph (6 isolated sensors without graph neighbours excluded).
- **25 local bottlenecks** are much slower than their neighbours in the PM peak (z ≤ −1.5) and congested ≥ 1.5× as often; worst 402117: -23.7 mph vs peers, 67.8% vs 5.5% congested.
- 46 sensors have low agreement with their peers (r < 0.6). Their data quality is only marginally different (0.62% vs 0.29% non-observed; Spearman ρ = -0.10), so low agreement more likely reflects genuinely local traffic conditions than detector faults, though detector drift cannot be ruled out.
- 10 sensors are consistently faster than their peers (z ≥ +1.5); 7 of them are directly downstream of a flagged bottleneck, where traffic discharges after the restriction.

## [Q9 — Combined congestion severity and burden](q9.md)

_Which sensors and days carry the greatest overall congestion burden when frequency, intensity, duration and travel-time reliability are combined, and is the ranking robust?_

- Highest burden: sensor **400069** (CSBI 0.87): 20.8% of working-day readings congested, 77.7% below free flow while congested, 237-min average episodes, PTI 6.5 (to be on time 95% of the time, a traveller must budget 6.5× the free-flow travel time).
- Burden is concentrated: the 10 highest-CSBI sensors (3.1% of sensors) carry 11.2% of all congested hours per working day; network peak TTI averages 1.44, with a mean excess of 23 s per mile in the peaks.
- The ranking is robust: dropping any single component keeps Spearman ρ ≥ 0.98 with the full index and at least 8/10 of the top 10.
- Components are related but not redundant (Spearman: frequency–unreliability 0.89, frequency–intensity 0.48, frequency–duration 0.86).

## [Q10 — Congestion onset and state transitions](q10.md)

_How does traffic move between free, slowed and congested states, how likely is a new congestion onset at each time and speed, and which signals precede an onset?_

- States are sticky: P(stay) = 0.983 free, 0.863 slowed, 0.922 congested, giving expected dwell times of 287, 36 and 64 min.
- Congestion is almost always entered via 'slowed': P(free → congested) = 1.3e-05 vs P(slowed → congested) = 0.0240 (1,791×).
- The first-order chain reproduces the state mix: stationary shares free 83.2%, slowed 12.8%, congested 3.9% vs observed free 83.2%, slowed 12.8%, congested 3.9%.
- Onset hazard on working days peaks at **17:00** (2.08% of non-congested readings tip into congestion within 5 min). By speed: 23.4% in the 35–40 mph band vs 0.001% at ≥ 60 mph.

## [Q11 — Congestion recovery performance across sensors](q11.md)

_Which sensors recover slowly from congestion, how consistent is their recovery, and does recovery depend on time of day or episode length?_

- 284 sensors have ≥ 10 sustained episodes. The median sensor recovers to free flow in **45 min**, but sensors range from 10 to 180 min (IQR across sensors 25–75 min).
- **47 slow-recovery sensors** are ≥ 1 sd slower than average *and* slower than their neighbours; worst 407373: median 180 min (peers 105 min), 51.9% of episodes not recovered within 180 min.
- Recovery depends on when an episode ends: median 180 min for episodes ending at 14:00 vs 15 min at 20:00 (hours with ≥ 200 episodes). Demand that remains high delays full recovery.
- Longer episodes recover faster (episode-level Spearman ρ = -0.37 between duration and recovery time), likely because long episodes typically end as evening demand falls, while short ones end inside busy periods that keep speeds below free flow. Across sensors, total congested time and median recovery have rank correlation -0.16.

## [Q12 — Concentration of congestion burden (Pareto analysis)](q12.md)

_How concentrated is the congestion burden across sensors, days and times, does the 80/20 rule hold, and how well would targeting the worst locations work on future data?_

- Across sensors the 80/20 rule is softened: the top 20% of sensors carry **52.1%** of congested minutes (Gini 0.52); half of all congestion sits on 19.1% of sensors, and 80% on 40.6%.
- Delay is less concentrated: top 20% of sensors carry 41.3% (Gini 0.35 vs 0.52), because moderate slowdowns at many sensors add delay without crossing the 35 mph threshold.
- In space-time the burden is far more concentrated: the top 10% of sensor × weekday × hour cells carry **95.9%** of congested minutes (Gini 0.94); 80% of the burden sits in 5.0% of cells.
- Across days the burden is evenly spread (Gini 0.37; top 10% of days = 18.9%), so congestion is a daily routine rather than a few extreme days.

## [Q13 — Missing data and sensor coverage bias](q13.md)

_Where and when are readings missing or invalid, is missingness related to congestion (so that rates and models are biased), and how evenly do sensors cover the network?_

- Data are 99.67% observed, 0.009% forward-filled and 0.32% missing. Almost all invalid readings come from stuck detectors (55,980), and they are concentrated: the worst 15 sensors hold 44.0% of all invalid readings.
- Missingness by time: working-day share ranges 0.19%–0.46% by hour (highest at 20:00); worst day 2017-05-19 (1.61%, 218 sensors affected on the worst day).
- There is a modest missing-not-at-random effect: hour-cells containing invalid readings have a congestion rate +0.57 pp (+20% relative) different from complete cells at the same hour (base 2.84%); invalid readings occur somewhat more often around congestion. Across sensors, Spearman ρ between invalid share and congestion rate = 0.01.
- Label loss: 1,294 stuck readings were below 35 mph (2.3% of stuck readings, 0.19% of all congested readings). They were removed as implausible flat lines and slightly understate congestion.

## [Q14 — Traffic anomalies versus sensor anomalies](q14.md)

_When a sensor reports an unusual speed, is it a real traffic event (seen by neighbouring sensors too) or a sensor problem (an isolated glitch)?_

- 748,806 anomalous readings (4.44% of 16,881,199 observed): **78.9% are traffic anomalies** corroborated by a neighbour, 19.1% isolated persistent, 0.9% isolated spikes and 1.2% unverifiable.
- Traffic anomalies are mostly slowdowns (81.9%) and cluster in peak hours (39.8% at 06–10/15–19h), while isolated anomalies are spread through the day (28.2% in the same hours).
- Isolated spikes are concentrated on few sensors: the 15 most spike-prone sensors carry 49.5% of all spikes.
- Spike rate and the invalid-reading share from cleaning are essentially unrelated across sensors (Spearman ρ = -0.02). Spikes therefore reveal a *different* kind of sensor problem than the stuck/zero readings found in cleaning.

## [Q15 — Simulated-stream throughput and latency benchmarks](q15.md)

_Can Spark Structured Streaming process the PEMS-BAY sensor stream fast enough for real-time 5-minute predictions on this machine, and which settings matter?_

- Peak throughput **13,096 readings/s** (40.5 five-minute slots per second) with 48 slots per micro-batch and 12 shuffle partitions: about **12,146× faster than real time** for the 325-sensor network (one slot arrives every 300 s).
- Micro-batch size matters: one slot per batch reaches only 469 readings/s (median batch 652 ms, dominated by fixed per-batch overhead: planning 24 ms, commit 125 ms).
- State-store partitions: 4 → 8,187 readings/s (batch p50 448 ms), 12 → 4,725 readings/s (batch p50 684 ms), 48 → 2,067 readings/s (batch p50 1828 ms). Each partition is a separate state store, so too many small partitions add overhead.
- End-to-end latency (file landed → window result in the sink) at 600× real-time arrival: median **0.83 s**, p95 1.42 s with the default trigger; 1.04 s / 2.32 s with a 2 s processing-time trigger. Both are far below the 5-minute prediction horizon.

## [Q16 — Five-minute-ahead congestion prediction: comparative metrics](q16.md)

_How well do Spark MLlib models predict whether a sensor will be congested in the next 5 minutes, compared with simple baselines, and where do they add value?_

- Best model on validation: **Logistic regression**. Test F1 **0.931** (95% CI 0.927–0.934), PR-AUC 0.973, ROC-AUC 0.999, precision 0.912, recall 0.950 at threshold 0.924.
- It beats persistence (F1 0.922) by **+0.008** F1 (95% CI +0.007 to +0.009; better in 100.0% of bootstrap resamples), a statistically clear gain.
- The value is in **state changes**: the model catches 37.9% of congestion onsets and correctly predicts 6.5% of clearances (persistence: 0% and 0% by construction); transition accuracy 22.2%, at a false-alarm rate of 0.08% on stable free-flow rows.
- The historical-profile baseline alone reaches F1 0.714 (PR-AUC 0.740): recurrence helps, but live state dominates at 5 minutes.

## [Q17 — Feature-group contribution and ablation](q17.md)

_Which groups of engineered features actually drive the 5-minute congestion predictions, and which are redundant?_

- With all 41 features the compact GBT reaches PR-AUC 0.978, F1 0.934 and onset recall 31.1% on the held-out validation days.
- Largest contribution: **lag** (removing it lowers PR-AUC by 0.004 and onset recall by 9.3 pp), followed by profile (0.001 PR-AUC).
- For predicting **onsets**, the most important group is **lag** (−9.3 pp onset recall when removed).
- Groups whose removal changes PR-AUC by less than 0.005: lag, profile, current, temporal, sensor, graph, rolling. They are redundant given the others, because their information is also carried by correlated features.

## [Q18 — Model generalization across time periods and sensors](q18.md)

_Does the selected model perform consistently across weeks, months, times of day, day types and individual sensors, and can it generalize to sensors it was not trained on?_

- Across 9 weeks (validation + test), weekly F1 ranges 0.923–0.937 (std 0.005) with a trend of -0.004 per 10 weeks: no meaningful drift.
- By month: 2017-05 F1 0.933 (gain +0.008), 2017-06 F1 0.932 (gain +0.008).
- By hour (test), the model adds most over persistence at 06:00 (+0.051 F1); F1 is lowest at 05:00 (0.725, 113 positives; hours with ≥ 100 positives). Day types: holiday 0.672, weekend 0.905, working day 0.931.
- Per sensor (264 sensors with ≥ 30 positives): median F1 0.916, IQR 0.852–0.949. Spearman ρ = 0.78 with the sensor's congestion rate, so the model is better on frequently congested sensors.
