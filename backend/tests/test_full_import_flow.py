import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_full_import_flow_upload_detect_map_validate_export():
    transport = ASGITransport(app=app)

    async with AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        csv_content = (
            "first_name,email,phone,signup_date,amount\n"
            "Josh,josh@example.com,1234567890,2024-01-15,$12.50\n"
            "Jane,jane@example.com,9876543210,2024-02-20,$22.75\n"
            "Bad,bad-email,123,not-a-date,$abc\n"
            "Josh,josh@example.com,1234567890,2024-01-15,$12.50\n"
        )

        upload_response = await client.post(
            "/api/upload",
            files={
                "file": (
                    "pipeline_full_flow.csv",
                    csv_content.encode("utf-8"),
                    "text/csv",
                )
            },
        )

        assert upload_response.status_code == 200

        upload_data = upload_response.json()
        file_id = upload_data["file_id"]

        assert upload_data["row_count"] == 4
        assert upload_data["col_count"] == 5

        detect_response = await client.get(f"/api/detect/{file_id}")

        assert detect_response.status_code == 200
        assert len(detect_response.json()["columns"]) == 5

        target_schema = [
            {"name": "first_name", "type": "string", "required": True, "variants": []},
            {"name": "email", "type": "email", "required": True, "variants": []},
            {"name": "phone", "type": "phone", "required": True, "variants": []},
            {"name": "signup_date", "type": "date", "required": True, "variants": []},
            {"name": "amount", "type": "float", "required": True, "variants": []},
        ]

        map_response = await client.post(
            f"/api/map/{file_id}",
            json={"target_schema": target_schema},
        )

        assert map_response.status_code == 200
        assert len(map_response.json()["mappings"]) == 5

        confirm_response = await client.post(
            f"/api/map/{file_id}/confirm",
            json={
                "confirmed_mappings": [
                    {"original": "first_name", "canonical": "first_name"},
                    {"original": "email", "canonical": "email"},
                    {"original": "phone", "canonical": "phone"},
                    {"original": "signup_date", "canonical": "signup_date"},
                    {"original": "amount", "canonical": "amount"},
                ]
            },
        )

        assert confirm_response.status_code == 200
        assert confirm_response.json()["confirmed"] is True

        validate_response = await client.get(f"/api/validate/{file_id}")

        assert validate_response.status_code == 200

        validation_data = validate_response.json()

        assert validation_data["total_rows"] == 4
        assert validation_data["clean_rows"] == 2
        assert validation_data["error_rows"] == 2
        assert validation_data["all_error_indices"] == [2, 3]

        error_types = {error["error_type"] for error in validation_data["errors"]}

        assert "INVALID_EMAIL" in error_types
        assert "INVALID_PHONE" in error_types
        assert "INVALID_DATE" in error_types
        assert "TYPE_MISMATCH" in error_types
        assert "DUPLICATE_ROW" in error_types

        export_response = await client.get(f"/api/export/{file_id}")

        assert export_response.status_code == 200
        assert "text/csv" in export_response.headers["content-type"]

        exported_csv = export_response.text

        assert "Josh,josh@example.com,1234567890,2024-01-15,$12.50" in exported_csv
        assert "Jane,jane@example.com,9876543210,2024-02-20,$22.75" in exported_csv
        assert "bad-email" not in exported_csv
        assert "$abc" not in exported_csv