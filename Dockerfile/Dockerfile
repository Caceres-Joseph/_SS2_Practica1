FROM jupyter/pyspark-notebook:latest
WORKDIR '/home/jovyan/work'

# Install from requirements.txt file
COPY requirements.txt /tmp/
RUN conda install --yes --file /tmp/requirements.txt && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

RUN pip install plotly==4.1.0

USER root

WORKDIR '/home/jovyan/work'
RUN chmod 777 -R /home/jovyan/