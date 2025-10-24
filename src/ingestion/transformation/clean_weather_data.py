from datetime import datetime


def clean_weather_data(weather_data, city_name="Athens"):
    """Καθαρίζει και αναδιαμορφώνει το raw weather JSON σε structured μορφή."""

    # creation of MetaData
    metadata = {
        "city": city_name,
        "latitude": weather_data.get("latitude"),
        "longitude": weather_data.get("longitude"),
        "timezone": weather_data.get("timezone"),
        "start_date": weather_data.get("start_date", "unknown"),
        "end_date": weather_data.get("end_date", "unknown"),
        "retrieved_at": datetime.utcnow().isoformat() + "Z",
    }

    # manipulation of hourly data
    hourly = weather_data.get("hourly", {})
    times = hourly.get("time", [])
    variable_names = [k for k in hourly.keys() if k != "time"]

    records = []
    for i, t in enumerate(times):
        record = {"time": t}
        for var in variable_names:
            values = hourly.get(var, [])
            if i < len(values):
                record[var] = values[i]
        records.append(record)

    # final structure
    clean_json = {"metadata": metadata, "data": records}

    return clean_json
