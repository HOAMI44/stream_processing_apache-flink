# Stream Processing Use Cases

## Pattern: Summary Statistics over Interval

> Calculate basic summary statistics (min, max, count, average) over a given time interval.

**Examples:**

- An welcher Station war der Wind in den letzten 10 Jahren am Stärksten? -> max over the interval [now - 10 years, now]

**Use Case:**

- 1 Rolling temperature statistics per station: Sliding/tumbling window (1 h or 24 h) per station → continuous min, max, and average temperature showing how temperature evolves over time.

## Pattern: Predicate Based Count

> Count the number of times a predicate evaluates to true/false over a given time interval.

**Examples:**

- Anzahl Tage Temp > zb. 30°? -> count P(a) = True over a time interval [t_start, t_end]

**Use Case:**

- 10 Station data-quality monitoring: Predicate value is missing OR invalid OR suspicious counted per station over a rolling window → identifies unreliable stations.

## Pattern: Summary Statistics on time dependent functions over Interval

> Calculate basic summary statistics (min, max, count, average) on values derived by functions that need multiple time shifted arguments over a given time interval.

**Examples:**

- An welcher Station war die Temperatur Steigung am steilsten in den letzten Jahren? -> max over a time interval [t_start, t_end] of a function f(x*t, x*(t-1))

**Use Case:**

- 2 Rapid temperature change detection: Rate of change computed as f(x*t, x*(t-1)) / Δt (°C/h) between consecutive readings → detects sudden warming or cooling events per station.

## Pattern: Classification of time interval

> Classify a certain time interval based on characteristics derived from the observed values within the interval.

**Examples:** \* Hitzeperioden bestimmen -> P(f(x*t,…,x*(t-n))

**Use Case:**

- 9 Identify data gaps: Classify an interval as complete or incomplete by checking for missing days/hours per station.

## Pattern: Summary Statistics derived by comparing values within multiple intervals

> Calculate basic summary statistics (min, max, count, average) of values derived by functions that take at least two sets of values as parameters.

**Examples:**

- Durchschnittliche Regendauer im Jahr

**Use Case:**

- 2 Compare Current Temperature With Historical Yearly Average: Current rolling window average compared against stored historical yearly average per station → stream of temperature deviations.

## Pattern: Summary Statistics based on ordered values over Interval

> Calculate summary statistics that rely on ordered values (e.g. median, percentiles) over a given time interval.

**Examples:**

- Median und Perzentile pro Jahr

**Use Case:**

- 7 Create a Temperature Histogram per Region: Temperature values bucketed into ranges and counted per region/window → distribution of cold/mild/hot measurements per region.

## Pattern: Simple Forecasting

> Calculate forecasts for a series of values based on historical values using simple methods such as moving averages, exponential smoothing, …

**Examples:**

- Was ist die voraussichtliche Durchschnittstemperatur in den nächsten 10 Jahren basierend auf der Steigung der letzten 10?

**Use Case:**

- 8 Climate Change Trend Monitoring: Moving averages over 30-day windows per station → detect long-term temperature trends in real time.

## Pattern: Stream Join

> Enrich or correlate a stream by joining it with another independent stream or a reference table, matching on time proximity and/or spatial proximity, to produce enriched records that combine information from both sources.

**Examples:**

- Wetterdaten mit Flugplänen joinen → Wetterbedingungen pro Flughafen und Flug

**Use Case:**

- 8 Skiing / Outdoor-Event-Optimierung: Weather stream joined with booking data by resort-ID + time range → real-time condition recommendations and automated warnings to app users.

## Pattern: Event Duration Tracking

> Detect when a measured value enters a defined state by crossing a threshold, track how long the state persists, and emit a result event when the state ends, carrying the start time, end time, and duration. Typically implemented via session windows or stateful open/close processors.

**Examples:**

- Wie lange war die Sichtweite unter 200 m? → session window on visibility < 200 m, emit {start, end, duration}

**Use Case:**

- 5 Low visibility duration tracking: Session window opens when visibility < threshold, closes when it recovers → emits bad-visibility period with start, end, and duration.

## Pattern: Pattern: Multi-Signal Composite Event Detection

> Detect a named complex event by combining multiple independent measurement dimensions or signal streams within a time window. No single signal is sufficient; the event is declared only when a defined combination of conditions holds simultaneously or in temporal sequence across signals.

**Examples:**

- Blizzard = Schneefall + Windgeschwindigkeit > 60 km/h + Sichtweite < 400 m innerhalb desselben Fensters

**Use Case:**

- 7 Waldbrand-Risikoindex: Temperature + humidity + wind + dryness combined into the Fire Weather Index per forest district per 1-h tumbling window → fire-risk alert when index exceeds threshold.
