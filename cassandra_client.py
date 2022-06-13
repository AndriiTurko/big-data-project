from cassandra.cluster import Cluster


class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)

    def execute(self, query):
        return self.session.execute(query)

    def close(self):
        self.session.shutdown()

    def insert_category_a(self, data):
        query = f"INSERT INTO category_a " \
                f"(id, domain, creation_time, user_id, user_name, page, is_bot) " \
                f"VALUES ('{data[0]}', '{data[1]}', '{data[2]}', " \
                f"'{data[3]}', '{data[4]}', '{data[5]}', " \
                f"'{data[6]}', '{data[7]}');"
        self.execute(query)

    def select_distinct_domains(self):
        query = f"SELECT DISTINCT domain FROM category_a"
        data = self.execute(query)
        return data

    def select_domain_statistics(self, domain, start_time, end_time):
        query = f"SELECT COUNT(domain) FROM category_a " \
                f"WHERE domain = '{domain}' AND " \
                f"creation_time >= '{start_time}' AND " \
                f"creation_time <= '{end_time}';"

        data = self.execute(query)

        return data

    def select_bot_statistics(self, domain):
        query = f"SELECT COUNT(domain) FROM category_a " \
                f"WHERE domain = '{domain}' AND " \
                f"is_bot = true;"

        data = self.execute(query)

        return data

    def select_pages_for_user(self, user_id):
        query = f"SELECT page FROM category_a " \
                f"WHERE user_id = '{user_id}';"

        data = self.execute(query)

        return data

    def select_distinct_users(self):
        query = f"SELECT DISTINCT user_id FROM category_a"
        data = self.execute(query)
        return data

    def select_top_users(self, user_id, start_time, end_time):
        query = f"SELECT COUNT(user_id) FROM category_a " \
                f"WHERE user_id = '{user_id}'" \
                f"creation_time >= '{start_time}' AND " \
                f"creation_time <= '{end_time}' " \
                f"ORDER BY COUNT(user_id) DESC" \
                f"LIMIT 20;"

        data = self.execute(query)

        return data

    def select_amount_of_pages_for_domain(self, domain):
        query = f"SELECT COUNT(page) FROM category_a " \
                f"WHERE user_id = '{domain}';"

        data = self.execute(query)

        return data

    def select_page(self, page_id):
        query = f"SELECT COUNT(page) FROM category_a " \
                f"WHERE user_id = '{[page_id]}';"

        data = self.execute(query)

        return data

    def select_users(self, user_id, start_time, end_time):
        query = f"SELECT COUNT(user_id) FROM category_a " \
                f"WHERE user_id = '{user_id}'" \
                f"creation_time >= '{start_time}' AND " \
                f"creation_time <= '{end_time}';"

        data = self.execute(query)

        return data

