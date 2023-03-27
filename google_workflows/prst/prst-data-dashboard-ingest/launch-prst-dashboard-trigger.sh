gcloud eventarc triggers create prst-gisaid-trigger-standard \
	--destination-workflow=prst-dashboard-standard \
	--destination-workflow-location=us-central1 \
	--event-filters="type=google.cloud.storage.object.v1.finalized" \
	--event-filters="bucket=pr-dashboard-gisaid-data" \
	--location=us \
	--service-account="932099266299-compute@developer.gserviceaccount.com"
