Running AWS commands locally
============================
1. Preferably create a virtual environment. (virtualenv)
2. pip install boto3.
3. Navigate to **C:\\Users\\<username>\\.aws** (make directory if it doesn't exist)
4. Create file "config", containing the text:

.. code-block:: console

  [default]
  region=ap-southeast-1

5. Generate your access keys @ https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html

  * Click on your name at the top right of the console > Security Credentials.
  * Scroll down to Access Keys.
  * Create Access Key.
  * Specify reason.
  * Download credentials.

6. Create file "credentials", containing the text:

.. code-block:: console

  [default]
  aws_access_key_id = <key>
  aws_secret_access_key = <secret_key>
