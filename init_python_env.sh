#!/usr/bin/env bash
git clone https://github.com/pyenv/pyenv.git ~/.pyenv

echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc

source ~/.bashrc

pyenv install 3.6.0
pyenv local 3.6.0

python -V

pip install elasticsearch python-daemon pymysql apscheduler

