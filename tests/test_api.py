"""Requires Postgres (docker compose up -d db) - see tests/conftest.py.
The queue backend is swapped to InMemoryQueueBackend via the `client`
fixture, so these tests don't need LocalStack running.
"""


def test_root_health_check(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "version" in response.json()


def test_list_sub_task_types(client, sub_task_type):
    response = client.get("/sub-task-type/")
    assert response.status_code == 200
    names = [t["name"] for t in response.json()]
    assert "quick" in names


def test_create_job_run_enqueues_tasks(client, sub_task_type, memory_queue):
    response = client.post("/job-run/", json={"case_id": "case-1", "sub_task_type_names": ["quick"]})
    assert response.status_code == 200
    body = response.json()
    assert body["case_id"] == "case-1"
    assert len(body["sub_tasks"]) == 1
    assert body["sub_tasks"][0]["status"] == "Pending"

    # One message was enqueued into the in-memory backend.
    messages = memory_queue.receive(10, 0)
    assert len(messages) == 1


def test_create_job_run_rejects_unknown_type(client):
    response = client.post("/job-run/", json={"case_id": "case-1", "sub_task_type_names": ["does-not-exist"]})
    assert response.status_code == 400


def test_get_sub_task(client, sub_task):
    response = client.get(f"/sub-task/{sub_task.task_id}")
    assert response.status_code == 200
    assert response.json()["task_id"] == sub_task.task_id


def test_get_sub_task_404(client):
    response = client.get("/sub-task/does-not-exist")
    assert response.status_code == 404
