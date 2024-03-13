gcloud eventarc triggers create prst-gisaid-ingest-trigger \
	--destination-workflow=prst-gisaid-ingest \
	--destination-workflow-location=us-central1 \
	--event-filters="type=google.cloud.storage.object.v1.finalized" \
	--event-filters="bucket=pr-dashboard-gisaid-data" \
	--location=us-central1 \
	--service-account="terra-batchie-watchie@general-theiagen.iam.gserviceaccount.com"
