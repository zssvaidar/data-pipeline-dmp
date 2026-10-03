FROM public.ecr.aws/docker/library/python:3.12-slim
RUN pip install --no-cache-dir --root-user-action=ignore duckdb==1.5.6 \
 && python -c "import duckdb; duckdb.sql('INSTALL httpfs')"
WORKDIR /app
COPY segment.py segment.sql ./
CMD ["python", "segment.py"]
