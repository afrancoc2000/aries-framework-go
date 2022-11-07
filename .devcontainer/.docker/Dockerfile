ARG GO_VERSION=
FROM mcr.microsoft.com/vscode/devcontainers/go:${GO_VERSION}
# We intentionally use multiple layers small so that startup is faster with parallel pull

# copy the stup scripts to the container
COPY library-scripts/*.sh /scripts/

# install packages
RUN export DEBIAN_FRONTEND=noninteractive && \
    sudo apt-get update
RUN sudo apt-get -y install --no-install-recommends apt-utils dialog
RUN sudo apt-get -y install --no-install-recommends apt-transport-https ca-certificates
RUN sudo apt-get -y install --no-install-recommends curl git wget nano bash-completion dos2unix
RUN sudo apt-get -y install --no-install-recommends software-properties-common make
RUN sudo apt-get -y install --no-install-recommends jq
RUN sudo apt-get -y install --no-install-recommends httpie
RUN sudo apt-get -y install --no-install-recommends gnupg
RUN sudo apt-get -y install --no-install-recommends lsb-release

# use scripts from: https://github.com/microsoft/vscode-dev-containers/tree/main/script-library to install tools
RUN /bin/bash /scripts/docker-in-docker-debian.sh
RUN /bin/bash /scripts/kubectl-helm-debian.sh
RUN /bin/bash /scripts/azcli-debian.sh

# Creating a volume for docker in docker
VOLUME [ "/var/lib/docker" ]

# install k9s, github cli, jp and fluxctl
RUN sudo curl -Lo ./k9s.tar.gz https://github.com/derailed/k9s/releases/download/v0.24.2/k9s_Linux_x86_64.tar.gz && \
    sudo mkdir k9s && \
    sudo tar xvzf k9s.tar.gz -C ./k9s && \
    sudo mv ./k9s/k9s /usr/local/bin/k9s && \
    sudo rm -rf k9s.tar.gz k9s

RUN VERSION=$(curl -i https://github.com/jmespath/jp/releases/latest | grep "location: https://github.com/" | rev | cut -f 1 -d / | rev | sed 's/\r//') && \
    sudo wget https://github.com/jmespath/jp/releases/download/$VERSION/jp-linux-amd64 -O /usr/local/bin/jp && \
    sudo chmod +x /usr/local/bin/jp && \
    sudo wget https://github.com/cli/cli/releases/download/v1.10.3/gh_1.10.3_linux_amd64.deb && \
    sudo dpkg -i gh_1.10.3_linux_amd64.deb && \
    sudo rm gh_1.10.3_linux_amd64.deb

# use multiple layers for updates
RUN sudo apt-get install -y azure-cli
RUN sudo apt-get install -y python3
RUN sudo apt-get upgrade -y

# install oh-my-bash
RUN cd $HOME && \
    OSH=$HOME/.oh-my-bash && \
    rm -rf $OSH && \
    git clone --depth=1 https://github.com/ohmybash/oh-my-bash $OSH && \
    cp $OSH/templates/bashrc.osh-template $HOME/.bashrc && \
    sed "/^export OSH=/ c\\export OSH=$OSH" $HOME/.bashrc > $HOME/.bashrc-ombtemp && \
    mv -f $HOME/.bashrc-ombtemp $HOME/.bashrc

# install npm
RUN curl -fsSL https://deb.nodesource.com/setup_16.x | sudo -E bash - && \
    apt-get install -y nodejs && \
    apt-get install -y chromium

ENV CHROME_BIN="/usr/bin/chromium"

# Updating bashrc (needed to append)
COPY bashrc /root/.bashrc_temp
RUN cat $HOME/.bashrc_temp >> $HOME/.bashrc && \
    cat $HOME/.bashrc_temp >> $HOME/.zshrc && \
    rm $HOME/.bashrc_temp

## Git Credential Manager
RUN wget https://github.com/GitCredentialManager/git-credential-manager/releases/download/v2.0.567/gcmcore-linux_amd64.2.0.567.18224.deb
RUN sudo dpkg -i gcmcore-linux_amd64.2.0.567.18224.deb
RUN git-credential-manager-core configure
