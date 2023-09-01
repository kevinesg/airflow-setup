#### * Instructions below assumes you are using a Linux distribution (some steps will be different for Windows or Mac users).

# multipass VM initial setup

## install multipass
1. Install `snapd` using your package manager. In my case, since I use the `apt` package manager, I enter `sudo apt install snapd`.
2. Enter `sudo snap install multipass` to install multipass.
3. Enter `multipass --help` to check the documentation.
4. Create a VM by entering `multipass launch --cpus 4 --memory 4GB --disk 20G --name airflow-vm`. Feel free to adjust the other specs and name.
5. Enter the VM by entering `multipass shell airflow-vm`.
6. Since this is an Ubuntu server distro, enter `sudo apt update; sudo apt upgrade` before installing additional packages.

## install docker compose
1. Enter `sudo apt install docker-compose` to install `docker compose`.
2. Enter `docker run hello-world` to check if docker has correct permissions already. If you encounter a `permission denied` error, follow the fix from [here](https://github.com/sindresorhus/guides/blob/main/docker-without-sudo.md). If you try `docker run hello-world` again, it should now run without an error.

## install miniconda
1. In your browser, go to [miniconda download page](https://docs.conda.io/en/latest/miniconda.html).
2. Look for `Miniconda3 Linux 64-bit` then right click and copy link address, so that we can install it on the VM.
3. Back to the VM terminal, enter `wget <link from previous step>`. It should download an `.sh` installer that you can check using `ls`.
4. Enter `bash <miniconda .sh installer filename>` to install miniconda. Follow and complete the instructions.
5. Enter `source .bashrc` to restart bashrc and apply conda changes.
6. Feel free to delete the installer afterwards.

## setup ssh connection
1. Generate an ssh key. Feel free to check the [instructions](https://cloud.google.com/compute/docs/connect/create-ssh-keys#linux-and-macos) from Google Cloud.
2. In your local machine terminal, enter `multipass info airflow-vm` to get the private IP address of the VM.
3. To ssh into the VM from your local machine, open a terminal from your local machine and enter `ssh ubuntu@<ip address>` where `<ip address>` is the private IP address from the previous step. Alternatively, you can edit (or create first if it doesn't exist yet) a `config` file under your `~/.ssh` directory, and add the following lines:

        Host NAME
        Hostname PRIVATE_IP_ADDRESS
        User ubuntu
        IdentityFile ~/.ssh/PRIVATE_SSH_KEY

    where

    - NAME is the name you want to use in the command ssh NAME to SSH into the VM
    - PRIVATE_IP_ADDRESS is from previous steps
    - PRIVATE_SSH_KEY is the file name of the private SSH key we created in step 1
    - in windows, you might need to enter the absolute path instead of relative path for the IdentityFile

4. Save changes to `config` file.
5. Add the public key counterpart of the private key you used in the previous steps. From your local machine terminal, enter `less ~/.ssh/<public key>`, copy the contents.
6. Go to your VM terminal and enter `nano ~/.ssh/authorized_keys`, enter on a newline the public key content from previous step, then save changes.
7. You can now ssh into the VM by entering `ssh <NAME>` in your local machine terminal.

# Airflow initial setup

## download the latest docker-compose.yaml
1. In your VM terminal, make sure you're in your chosen airflow directory (or create one if you prefer).
2. Go to [this page of the Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) (or look it up in case the link does not work anymore), look for "Fetching docker-compose.yaml" section, and copy the `curl` command. In my case, it is `curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.0/docker-compose.yaml'`. This will download the docker-compose.yaml for the latest Airflow version. In my case, the latest version is 2.7.0. The next steps are just following the instructions in the documentation.
4. Enter `mkdir -p ./dags ./logs ./plugins ./config` and `echo -e "AIRFLOW_UID=$(id -u)" > .env`. This will create the directories expected by Airflow, with proper permissions.

## initialize the database and run Airflow
1. enter `docker compose up airflow-init` (or in my case, since I installed `docker-compose` [an older `docker compose version`], `docker-compose up airflow-init`). Wait for the initialization to complete.
2. Enter `docker compose up -d` (or `docker-compose up -d`) to run Airflow.
3. Enter `docker ps` and check if the docker images are up and running.
4. Feel free to read the rest of the Airflow documentation.


#
I did the steps above to initialize this this repository. Everything else are files I manually created.
- I extended the default `docker-compose.yaml` from Airflow by creating a `requirements.txt` that contains the needed Python libraries, as well as a `Dockerfile` basically for installing the packages from `requirements.txt`. I edited `docker-compose.yaml` to be able extend it. For more detailed information, feel free to check the Airflow docs.
- `airflow/config/` expects two files: a GCP service account JSON file and a mediastack API key in a txt file.
- Airflow expects the DAGs to be inside the `dags/` directory.
- The DAGs in this repository use TaskFlow API instead of Operators, and I placed the task functions inside `dags/tasks/`.