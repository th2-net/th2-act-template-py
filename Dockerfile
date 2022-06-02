FROM python:3.8-slim
WORKDIR /usr/src/app
COPY . .
RUN pip install -r requirements.txt
ENV PYTHONPATH "${PYTHONPATH}:/usr/src/app"
ENTRYPOINT [ "python", "./th2_act_template/main.py"]