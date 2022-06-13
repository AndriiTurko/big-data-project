from datetime import datetime, timedelta

from cassandra_client import CassandraClient
from flask import Flask, request

app = Flask(__name__)


@app.route("/pages_by_domain", methods=['POST'])
def get_domain_statistics():
    json_data = request.get_json()
    response = []

    last_hour = int(datetime.strftime(datetime.now(), "%H"))
    start_times = [i for i in range(last_hour - json_data['hours']-1, last_hour)]

    client = CassandraClient()
    client.connect()

    dom_data = client.select_distinct_domains()
    for start_time in start_times:
        time_diff = last_hour - start_time
        start_time_converted = datetime.now() - timedelta(hours=time_diff)
        end_time_converted = datetime.now() - timedelta(hours=time_diff-1)
        time_dict = {"time_start" : start_time_converted,
                     "time_end": end_time_converted,
                     "statistics": []}
        for dom in dom_data:
            temp_data = client.select_domain_statistics(dom,
                                                        start_time_converted,
                                                        end_time_converted)
            time_dict["statistics"].append(temp_data[0])

        response.append(time_dict)

    client.close()
    return {"response": response}


@app.route("/bots_by_domain", methods=['POST'])
def get_domain_bots_statistics():
    json_data = request.get_json()

    current_time = datetime.now()

    start_time_converted = current_time - timedelta(hours=json_data['hours'] + 1)
    end_time_converted = current_time - timedelta(hours=json_data['hours'])

    response = {"time_start": start_time_converted,
                "time_end": end_time_converted,
                "statistics": []}

    client = CassandraClient()
    client.connect()

    dom_data = client.select_distinct_domains()
    for dom in dom_data:
        temp_data = client.select_domain_statistics(json_data['domain'],
                                                    start_time_converted,
                                                    end_time_converted)
        response["statistics"].append({"domain": dom, "created_by_bots": temp_data[0]})

    client.close()
    return {"response": response}


@app.route("/top_20_users", methods=['POST'])
def get_top_20_users():
    json_data = request.get_json()

    current_time = datetime.now()

    start_time_converted = current_time - timedelta(hours=json_data['hours'] + 1)
    end_time_converted = current_time - timedelta(hours=json_data['hours'])

    response = {"time_start": start_time_converted,
                "time_end": end_time_converted,
                "statistics": []}

    client = CassandraClient()
    client.connect()

    users = client.select_distinct_users()
    for user_id in users:
        temp_data = client.select_domain_statistics(user_id,
                                                    start_time_converted,
                                                    end_time_converted)
        response["statistics"].append({"user_id": user_id,
                                       "user_name": temp_data[0],
                                       "pages": temp_data[0]})

    client.close()
    return {"response": response}
