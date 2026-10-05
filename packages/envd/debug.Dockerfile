FROM golang:1.26.8

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update
RUN apt-get install systemd ca-certificates make python-is-python3 python3 nodejs npm chrony sudo -y

RUN update-ca-certificates

# Remove password for root.
RUN passwd -d root

# Create default user.
RUN useradd -ms /bin/bash user
RUN usermod -aG sudo user
RUN passwd -d user
RUN echo "user ALL=(ALL:ALL) NOPASSWD: ALL" >>/etc/sudoers

RUN mkdir -p /home/user
RUN chmod 777 -R /home/user
RUN chmod 777 -R /usr/local

RUN go install github.com/go-delve/delve/cmd/dlv@latest

WORKDIR /

RUN mkdir -p /etc/systemd/system/multi-user.target.wants
RUN ln -s /etc/systemd/system/envd.service /etc/systemd/system/multi-user.target.wants/envd.service

RUN mkdir -p /etc/systemd/system/serial-getty@ttyS0.service.d

RUN echo ' \n\
[Service] \n\
ExecStart= \n\
ExecStart=-/sbin/agetty --noissue --autologin root %I 115200,38400,9600 vt102 \n\
' >/etc/systemd/system/serial-getty@ttyS0.service.d/autologin.conf

RUN echo ' \n\
[Unit] \n\
Description=Env Daemon Service \n\
[Service] \n\
Type=simple \n\
Restart=always \n\
User=root \n\
Group=root \n\
ExecStart=/bin/bash -l -c /usr/bin/envd \n\
[Install] \n\
WantedBy=multi-user.target \n\
' >/etc/systemd/system/envd.service

RUN systemctl enable envd

RUN mkdir -p /etc/chrony
RUN echo ' \n\
makestep 1 -1 \n\
' >/etc/chrony/chrony.conf

RUN systemctl enable chrony 2>&1

COPY bin/envd /usr/bin/envd

WORKDIR /
