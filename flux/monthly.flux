// InfluxDB2 Flux example: last 31 days production
//
// Display the past 30 days production along with the current day production to date.  This requires two
// queries, the first to extract the last 30 days of production and a second query to calculate
// the production to date for the current day.
//
// Because the production for a given day requires that the total at the beginning of the day be
// subtracted from the total at the beginning of the following day, a sort is done to reverse the table
// so the time field will be the 1st of the month for the months production (otherwise it would tagged with
// the start of the following month).  So the results are sorted, differences taken, and sorted back to build
// the table for display.  Since the table will be in Watt-hours (Wh) and negative from the initial sort, the
// values are adjusted by -1000 to fix the sign and convert to kiloWatt-hours (kWh).
//
// The second query is similar but two fields are needed, 'today' will find the production at the start of
// the day and 'total' will extract the last reported production, subtract and adjust for the current
// days production.
//
// InfluxDB 1.8.x users should add the retention policy to the bucket name, ie, 'multisma2/autogen'

import "date"

// This can be used to align results with x-axis labels
timeAxisShift = 0h

past_30 = from(bucket: "multisma2")
  |> range(start: -32d)
  |> filter(fn: (r) => r._measurement == "production" and r._inverter == "site" and r._field == "midnight")
  |> sort(columns: ["_time"], desc: true)
  |> difference()
  |> map(fn: (r) => ({ _time: r._time, total: float(v: r._value) / -1000.0, _field: "total_wh" }))
  |> sort(columns: ["_time"], desc: false)
  |> yield(name: "past_30")

midnight = from(bucket: "multisma2")
  |> range(start: -1d)
  |> filter(fn: (r) => r._measurement == "production" and r._inverter == "site" and r._field == "midnight")
  |> filter(fn: (r) => date.monthDay(t: r._time) == date.monthDay(t: now()) )
  |> map(fn: (r) => ({ _time: r._time, total: r._value, _field: "total_wh" }))
  |> yield(name: "midnight")

right_now = from(bucket: "multisma2")
  |> range(start: -1d)
  |> filter(fn: (r) => r._measurement == "production" and r._inverter == "site" and r._field == "total_wh")
  |> last()
  |> map(fn: (r) => ({ _time: r._time, total: r._value, _field: "total_wh" }))
  |> yield(name: "right_now")

this_day = union(tables: [midnight, right_now])
  |> difference(nonNegative: false, columns: ["total"])
  |> map(fn: (r) => ({ _time: r._time, total: float(v: r.total) / -1000.0, _field: "total_wh" }))
  |> yield(name: "this_day")

union(tables: [past_30, this_day])
  |> sort(columns: ["_time"], desc: false)
  |> map(fn: (r) => ({ _time: r._time, _value: r.total, _field: "total_wh" }))
  |> timeShift(duration: timeAxisShift, columns: ["_time"])
  |> yield(name: "combined")