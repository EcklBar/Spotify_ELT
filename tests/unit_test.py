def test_spotify_client_id(spotify_client_id):
    assert spotify_client_id == "MOCK_CLIENT_ID"


def test_spotify_client_secret(spotify_client_secret):
    assert spotify_client_secret == "MOCK_CLIENT_SECRET"


def test_artist_names(artist_names):
    assert artist_names == '["Mock Artist"]'


def test_mock_postgres_conn_vars(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_name"
    

def test_dags_integrity(dagbag):
    # 1. No import errors
    assert dagbag.import_errors == {}, f"Import errors found: {dagbag.import_errors}"
    print("===========")
    print(dagbag.import_errors)
    
    # 2. All 3 DAGs exist
    expected_dag_ids = ["produce_json", "update_db", "data_quality"]
    loaded_dag_ids = list(dagbag.dags.keys())
    print("===========")
    print(dagbag.dags.keys())

    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_ids, f"DAG {dag_id} is missing."

    # 3. Exactly 3 DAGs
    assert dagbag.size() == 3
    print("===========")
    print(dagbag.size())

    # 4. Correct task counts
    expected_task_counts = {
        "produce_json": 6,
        "update_db": 3,
        "data_quality": 2,
    }
    print("===========")
    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_counts[dag_id]
        actual_count = len(dag.tasks)
        assert (
            expected_count == actual_count
        ), f"DAG {dag_id} has {actual_count} tasks, expected {expected_count}."
        print(dag_id, len(dag.tasks))