gcloud eventarc triggers create helix-gisaid-trigger \
	--destination-workflow=helix-gisaid \
	--destination-workflow-location=us-central1 \
	--event-filters="type=google.cloud.storage.object.v1.finalized" \
	--event-filters="bucket=cdph_helix_gisaid" \
	--location=us \
	--service-account="551108248392-compute@developer.gserviceaccount.com"
