FROM ubuntu:latest
RUN apt-get update && apt-get install -y sudo wget vim
WORKDIR /opt
RUN wget https://repo.anaconda.com/archive/Anaconda3-2023.03-1-Linux-x86_64.sh
RUN sh /opt/Anaconda3-2023.03-1-Linux-x86_64.sh -b -p /opt/anaconda3
RUN rm -f Anaconda3-2023.03-1-Linux-x86_64.sh

ENV PATH /opt/anaconda3/bin:$PATH

RUN pip install --upgrade pip
# RUN conda install snowflake-snowpark-python
RUN . /root/.bashrc && \
    conda init bash && \
    conda create --name py38_env --override-channels -c https://repo.anaconda.com/pkgs/snowflake python=3.8 numpy pandas && \
    conda activate py38_env
RUN conda install notebook
RUN conda install snowflake-snowpark-python


WORKDIR /