// InfluxDB2 Flux example: last 13 months production
//
// Display the past 12 month production along with the current month to date.  This requires two
// queries, the first to extract the last 12 months of production and a second query to calculate
// the production to date for the current month.
//
// Because the production for a given month requires that the total at the beginning of the month be
// subtracted from the total at the beginning of the following month, a sort is done to reverse the table
// so the time field will be the 1st of the month for the months production (otherwise it would tagged with
// the start of the following month).  So the results are sorted, differences taken, and sorted back to build
// the table for display.  Since the table will be in Watt-hours (Wh) and negative from the initial sort, the
// values are adjusted by -1000 to fix the sign and convert to kiloWatt-hours (kWh).
//
// The second query is similar but two fields are needed, 'today'will find the production at the start of
// the month and 'total' will extract the last reported production, subtract and adjust for the current
// month production.
//
// InfluxDB 1.8.x users should add the retention policy to the bucket name, ie, 'multisma2/autogen'

import "date"

// This can be used to align results with x-axis labels
timeAxisShift = -0d

// Extract the last 12 months of production
past_12 = from(bucket: "multisma2")
  |> range(start: -13mo)
  |> filter(fn: (r) => r._measurement == "production" and r._inverter == "site" and r._field == "midnight")
  |> filter(fn: (r) => date.monthDay(t: r._time) == 1)
  |> sort(columns: ["_time"], desc: true)
  |> difference()
  |> map(fn: (r) => ({ _time: r._time, _kwh: float(v: r._value) * -0.001 }))
  |> sort(columns: ["_time"], desc: false)
  |> timeShift(duration: timeAxisShift, columns: ["_time"])
  |> yield(name: "past_12")

// Extract the current months of production
first_of_month = from(bucket: "multisma2")
  |> range(start: -32d)
  |> filter(fn: (r) => r._measurement == "production" and r._inverter == "site" and r._field == "midnight")
  |> filter(fn: (r) => date.monthDay(t: r._time) == 1 )
  |> map(fn: (r) => ({ _time: r._time, _total_wh: r._value }))
  |> yield(name: "first_of_month")

today = from(bucket: "multisma2")
  |> range(start: -2h)
  |> filter(fn: (r) => r._measurement == "production" and r._inverter == "site" and r._field == "total_wh")
  |> last()
  |> map(fn: (r) => ({ _time: r._time, _total_wh: r._value }))
  |> yield(name: "today")

this_month = union(tables: [first_of_month, today])
  |> sort(columns: ["_time"], desc: true)
  |> difference(nonNegative: false, columns: ["_total_wh"])
  |> map(fn: (r) => ({ _time: r._time, _kwh: float(v: r._total_wh) * -0.001 }))
  |> timeShift(duration: timeAxisShift, columns: ["_time"])
  |> yield(name: "this_month")

// Combine the results in to a single table with '_value' and '_time' as the axes
union(tables: [past_12, this_month])
  |> sort(columns: ["_time"], desc: false)
  |> timeShift(duration: timeAxisShift, columns: ["_time"])
  |> yield(name: "combined")