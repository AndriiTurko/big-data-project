from datetime import datetime, timedelta

from cassandra_client import CassandraClient
from flask import Flask, request

app = Flask(__name__)

host = 'cassandra-node'
port = 9042
keyspace = 'wikipedia-project'


@app.route("/pages_by_domain", methods=['POST'])
def get_domain_statistics():
    json_data = request.get_json()
    response = []

    last_hour = int(datetime.strftime(datetime.now(), "%H"))
    start_times = [i for i in range(last_hour - json_data['hours']-1, last_hour)]

    client = CassandraClient(host, port, keyspace)
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
            temp_data = client.select_domain_statistics(dom[0],
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

    client = CassandraClient(host, port, keyspace)
    client.connect()

    dom_data = client.select_distinct_domains()
    for dom in dom_data:
        temp_data = client.select_domain_statistics(dom[0],
                                                    start_time_converted,
                                                    end_time_converted)
        response["statistics"].append({"domain": dom[0], "created_by_bots": temp_data[0]})

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

    client = CassandraClient(host, port, keyspace)
    client.connect()

    users = client.select_distinct_users()
    for user_id in users:
        temp_data = client.select_top_users(user_id[0],
                                            start_time_converted,
                                            end_time_converted)
        response["statistics"].append({"user_id": user_id[0],
                                       "user_name": temp_data[1],
                                       "pages": temp_data[0]})

    client.close()
    return {"response": response}


@app.route("/b_1", methods=['POST'])
def get_b1():
    client = CassandraClient(host, port, keyspace)
    client.connect()

    domains = client.select_distinct_domains()

    client.close()

    return domains


@app.route("/b_2", methods=['POST'])
def get_b2():
    json_data = request.get_json()

    client = CassandraClient(host, port, keyspace)
    client.connect()

    pages = client.select_pages_for_user(json_data["user_id"])

    client.close()

    return pages[0]


@app.route("/b_3", methods=['POST'])
def get_b3():
    json_data = request.get_json()

    client = CassandraClient(host, port, keyspace)
    client.connect()

    pages = client.select_amount_of_pages_for_domain(json_data["domain"])

    client.close()

    return pages[0]


@app.route("/b_4", methods=['POST'])
def get_b4():
    json_data = request.get_json()

    client = CassandraClient(host, port, keyspace)
    client.connect()

    page = client.select_page(json_data["page_id"])

    client.close()

    return page[0]


@app.route("/b_5", methods=['POST'])
def get_b5():
    json_data = request.get_json()

    current_time = datetime.now()

    start_time_converted = current_time - timedelta(hours=json_data['hours'] + 1)
    end_time_converted = current_time - timedelta(hours=json_data['hours'])

    response = {"time_start": start_time_converted,
                "time_end": end_time_converted,
                "statistics": []}

    client = CassandraClient(host, port, keyspace)
    client.connect()

    users = client.select_distinct_users()
    for user_id in users:
        temp_data = client.select_users(user_id[0],
                                        start_time_converted,
                                        end_time_converted)
        response["statistics"].append({"user_id": user_id[0],
                                       "user_name": temp_data[1],
                                       "pages": temp_data[0]})

    client.close()
    return {"response": response}
