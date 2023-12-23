from opensearchpy import OpenSearch
class OpenSearchClient(object):
    _instance = None
    _client = None

    def __new__(cls, host, port, auth, ca_certs_path):
        if cls._instance is None:
            cls._instance = super(OpenSearchClient, cls).__new__(cls)
            # Put any initialization here.
            cls._client = OpenSearch(
                hosts=[{'host': host, 'port': port}],
                http_compress=True,  # enables gzip compression for request bodies
                http_auth=auth,
                # client_cert = client_cert_path,
                # client_key = client_key_path,
                use_ssl=True,
                verify_certs=False,
                ssl_assert_hostname=False,
                ssl_show_warn=False,
                ca_certs=ca_certs_path
            )
        return cls._instance

    def write(self, id, doc: dict):
        self._client.index(
            index="resume-index-1",
            body=doc,
            id=id,
            refresh=True)


# if __name__ == "__main__":
#     opensearch_client = OpenSearchClient("localhost", 9200, ("admin", "admin"), "root-ca.pem")
#     opensearch_client.write({"ref_id": 2})
