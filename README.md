# vehicle-gateway-c_car_platform

Vehicle gateway service container.

Phase 3 MVP adds a **minimal, optional** telematics publisher stub that can:
- validate TelematicsV1 payloads using `shared-c_car_platform`
- publish to **Kafka** topic `telematics.v1`, or
- fall back to **HTTP** batch ingest on `telematics-ingestion-c_car_platform`

This is intentionally minimal and **feature-flagged** so it does not interfere with preview lifecycle.

## Local development

### Environment
Copy and edit:
- `.env.example` -> `.env`

To enable publishing:
- `TELEMATICS_PUBLISH_ENABLED=true`
- Choose:
  - `TELEMATICS_PUBLISH_MODE=kafka` (default), or
  - `TELEMATICS_PUBLISH_MODE=http`

### Run
```bash
npm install
npm run dev
```

## API (MVP)

### Health
`GET /health`

### Dev publish endpoint
`POST /v1/dev/telematics/publish`

Body is a TelematicsV1 JSON payload:
```json
{
  "schemaVersion": "v1",
  "vehicleId": "VIN123",
  "timestamp": "2026-01-21T00:00:00.000Z",
  "speedKph": 10
}
```

- If `TELEMATICS_PUBLISH_MODE=kafka`, message is produced to Kafka topic `telematics.v1`.
- If `TELEMATICS_PUBLISH_MODE=http`, gateway calls `telematics-ingestion` batch endpoint.

> Note: This endpoint is for MVP/local simulation only.
