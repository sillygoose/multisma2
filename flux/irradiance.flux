// InfluxDB2 Flux example
// Display clearsky irradiance and site production in a Graph visualization
// The SMA inverters store production as the total production in Wh since commissioning, typically
// every 5 minutes.  To convert this data to Watts (W), you must find the difference between adjacent
// data points (Wh) and multiply by 3600 secs/hour (Ws) and then divide by the interval in seconds).
//
// InfluxDB 1.8.x users should add the retention policy to the bucket name, ie, 'multisma2/autogen'

// Collect the site total production data
from(bucket: "multisma2")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "production" and r._inverter == "site" and r._field == "total_wh")
  |> elapsed(unit: 1s)
  |> difference(nonNegative: true, columns: ["_value"])
  |> filter(fn: (r) => r._value > 0)
  |> map(fn: (r) => ({ _time: r._time, site: float(v: r._value) * 3600.0 / float(v: r.elapsed) }))
  |> drop(columns: ["elapsed", "_start", "_stop", "_field", "_inverter", "_measurement"])
  |> yield(name: "production")

site_area = 144.0
site_efficiency = 0.1725

from(bucket: "multisma2")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "sun" and r._field == "irradiance")
  |> map(fn: (r) => ({ _time: r._time, _value: r._value * site_area * site_efficiency }))
  |> yield(name: "irradiance")
