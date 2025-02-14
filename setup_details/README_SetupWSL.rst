Installing Python3.? on Windows
===============================
| If you are unable to make use of Cloud9/EC2, you can use Windows Subsystem for Linux to compile the layers.
| This documentation may be incomplete for Python3.10, as my version of Ubuntu 22.04 already comes with it.
|
Python 3.10
-----------
Python 3.10 already comes preinstalled with WSL, but you need to install pip.

.. code-block:: console

  sudo apt update && sudo apt upgrade

.. code-block:: console

  sudo apt install python3-pip
  sudo apt install zip

Python 3.12
-----------
Instructions taken from `here <https://www.linuxtuto.com/how-to-install-python-3-12-on-ubuntu-22-04/>`_.

.. code-block:: console

  sudo apt install software-properties-common -y
  sudo add-apt-repository ppa:deadsnakes/ppa

Press Enter.

.. code-block:: console

  sudo apt update && sudo apt install python3.12

.. code-block:: console

  curl -sS https://bootstrap.pypa.io/get-pip.py | sudo python3.12

This step breaks pip for python3.10, to fix, run:

.. code-block:: console

  curl -sS https://bootstrap.pypa.io/get-pip.py | sudo python3.10


Layer Creation
--------------
cd to the location on your Windows machine where the requirements.txt file resides. The starting point for the Windows filesystem is at **/mnt/**.

.. code-block:: console

  python3.10 -m pip install -r requirements.txt -t python/lib/python3.10/site-packages

OR

.. code-block:: console

  python3.12 -m pip install -r requirements.txt -t python/lib/python3.12/site-packages

Zipping can be done with your Windows utility of choice, or:

.. code-block:: console

  rm layer_latest.zip & zip -q -r layer_latest.zip python

Currently Used Libraries
========================
| Once you've pip installed what you need, delete <virtualenv_name>/python/bin. This helps fit the measly 262144000 bytes layer size limit.
| Deleting the bin prevents pipping of anything else, so make sure you have everything you need installed before deleting.
| Adding to the utils file after deletion still works.
| If you need to install more libraries, delete the virtualenv and start again from **Venv Creation**.
|
* pymysql
* pandas
* stream-unzip
* stream-zip
* to-file-like-obj
