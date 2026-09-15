# How to get environment on arrhenius:

HOME: /home/<user_name>
PROJECT REPO: /nobackup/proj/disk/luftlab/personal/<user_name>

**In home:**
Download conda from here: https://www.anaconda.com/docs/getting-started/miniconda/install/linux-install#wget

```
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
```

Install it: 
```
bash ~/Miniconda3-latest-Linux-x86_64.sh

```
Activate changes:
```
source .bashrc
```
Now install basic packages we will need:
Run:
```
conda config --add channels conda-forge
conda config --set channel_priority strict
conda create -n analysis -c conda-forge \
  python=3.11 \
  numpy scipy pandas \
  xarray netcdf4 dask \
  matplotlib cartopy seaborn \
  jupyterlab ipykernel \
  cftime nc-time-axis bottleneck \
  scikit-learn \
  tqdm
```

Now activate it: 

```
conda activate analysis
```


## ALT 1: TO ACCESS YOUR JUPYTER LAB:

```bash
ssh -L 8887:localhost:8887 <user_name>@login.hpc.arrhenius.naiss.se
conda activate analysis
cd <PROJECT_REPO>
jupyter-lab --port 8889
```

On your computer in browser, open http://localhost:8887/lab/workspaces/auto-D. 



## ALT 2: Use via VSC

Now you may open a notebook in VSC and click the little Kernel place to the top right

<img width="803" height="259" alt="image" src="https://github.com/user-attachments/assets/b4971a34-0296-4ba9-9ded-9f6cddcaedb5" />

next click "Select another kernel" then "Python Environments", then "Create Python environment" and "Enter interpreter path..." and enter "/home/<user_name>/miniconda3/envs/analysis/bin/python". 

You should be good to go!
