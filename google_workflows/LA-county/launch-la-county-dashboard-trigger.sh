gcloud eventarc triggers create la-county-gisaid-trigger-standard \
	--destination-workflow=la-county-gisaid-ingest \
	--destination-workflow-location=us-central1 \
	--event-filters="type=google.cloud.storage.object.v1.finalized" \
	--event-filters="bucket=la-county-gisaid-data-upload" \
	--location=us \
	--service-account="932099266299-compute@developer.gserviceaccount.com"
