Installing Python3.10 on Cloud9
===============================
| Cloud9 is used to compile layers that are `compatible <https://docs.aws.amazon.com/lambda/latest/dg/packaging-layers.html>`_ with the AWS environment. 
| This document is no longer being updated, future compilation is done on Windows Subsystem for Linux.
|
Cloud9 environment
------------------
1. Head to the AWS Cloud9 Console, and create an environment.
2. Use t2.micro (1 GiB RAM + 1 vCPU) and Amazon Linux 2.
3. Create and wait.

OpenSSL (needed for pip to work)
--------------------------------
| https://stackoverflow.com/questions/73407527/installing-ssl-package-with-pip-requires-ssl-package-to-be-already-installed
|

.. code-block:: console

  sudo yum update
  sudo yum install openssl-devel bzip2-devel libffi-devel

.. code-block:: console

  cd /usr/src
  sudo wget https://ftp.openssl.org/source/openssl-1.1.1q.tar.gz --no-check-certificate
  sudo tar -xzvf openssl-1.1.1q.tar.gz

.. code-block:: console

  cd openssl-1.1.1q
  sudo ./config --prefix=/usr --openssldir=/etc/ssl --libdir=lib no-shared zlib-dynamic
  sudo make
  sudo make install

Python3.10
----------
| https://awstip.com/create-aws-lambda-layers-using-cloud-9-694895903ca5?gi=39ef188a972c
|

.. code-block:: console

  cd /home/ec2-user/environment
  wget https://www.python.org/ftp/python/3.11.7/Python-3.10.13.tgz
  tar xvf Python-3.10.13.tgz

.. code-block:: console

  cd Python-*/
  ./configure --enable-optimizations --with-openssl=/usr
  sudo make altinstall
  cd ..

.. code-block:: console

  alias python=python3.10
  alias pip=pip3.10
  pip install virtualenv

Uninstall Python3.10 (if clean install needed)
----------------------------------------------
.. code-block:: console

  sudo rm -rf /usr/local/bin/python3.10
  sudo rm -rf /usr/local/bin/python3.10-config
  sudo rm -rf /usr/local/lib/python3.10

Venv Creation
=============
.. code-block:: console

  virtualenv python

Enter / Exit venv
-----------------
.. code-block:: console

  source python/bin/activate

.. code-block:: console

  deactivate

Currently Used Libraries
========================
| Once you've pip installed what you need, delete <virtualenv_name>/python/bin. This helps fit the measly 262144000 bytes layer size limit.
| Deleting the bin prevents pipping of anything else, so make sure you have everything you need installed before deleting.
| Adding to the utils file after deletion still works.
| If you need to install more libraries, delete the virtualenv and start again from **Venv Creation**.
|
* pymysql
* pandas

Zipping
=======
.. code-block:: console

  rm layer.zip & zip -q -r layer.zip python

Upload to Bucket
================
Ctrl-P + AWS: Upload Files...
