gcloud eventarc triggers create louisisana-gisaid-trigger-standard \
	--destination-workflow=louisiana-dashboard-standard \
	--destination-workflow-location=us-central1 \
	--event-filters="type=google.cloud.storage.object.v1.finalized" \
	--event-filters="bucket=louisiana-gisaid-data" \
	--location=us \
	--service-account="551108248392-compute@developer.gserviceaccount.com"
