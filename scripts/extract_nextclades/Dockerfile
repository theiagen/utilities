FROM us-docker.pkg.dev/general-theiagen/theiagen/theiaphylo:0.1.8 

COPY extract_nextclades.py /app/

RUN python3 -m pip install pandas --break-system-packages

RUN chmod +x /app/*
ENV PATH="/app:${PATH}"

# Set environment variables for Google Cloud SDK
ENV CLOUDSDK_CONFIG=/gcp/config
ENV PYTHONPATH=/app

CMD ["extract_nextclades.py"]