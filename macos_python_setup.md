# MacOS Python Environment Setup for AirglowRSSS

## Prerequisites
- Fresh install of macOS Sequoia 15.6 on Mac mini M4
- Administrator access to install software

## Getting Started
1. **Open Terminal:** Press `Cmd + Space` to open Spotlight search, type "Terminal", and press Enter
2. **Note about passwords:** When prompted for a sudo password, use your Mac login password (the password you use to log into your Mac)

## Step 1: Install Command Line Tools and Homebrew

1. **Install Xcode Command Line Tools:**
   ```bash
   xcode-select --install
   ```
   Click "Install" when the dialog appears.

2. **Install Homebrew:**
   ```bash
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   ```

3. **Add Homebrew to PATH** (add these lines to your shell profile):
   ```bash
   echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zshrc
   source ~/.zshrc
   ```

## Step 2: Install Python Environment Manager

1. **Install Miniconda:**
   ```bash
   brew install miniconda
   ```

2. **Initialize conda:**
   ```bash
   conda init zsh
   ```

3. **Restart your terminal** or run:
   ```bash
   source ~/.zshrc
   ```

## Step 3: Create Project Directory and Clone Repository

1. **Create local directories:**
   ```bash
   mkdir -p ~/src
   mkdir -p ~/notebooks
   cd ~/src
   ```

2. **Clone the AirglowRSSS repository:**
   ```bash
   git clone https://github.com/AirglowRSSS/airglowrsss.git
   cd airglowrsss
   ```

## Step 4: Create and Configure Python Environment

1. **Accept conda Terms of Service:**
   ```bash
   conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main
   conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r
   ```

2. **Create conda environment:**
   ```bash
   conda create -n airglowrsss python=3.12 -y
   ```

2. **Activate the environment:**
   ```bash
   conda activate airglowrsss
   ```

3. **Update conda:**
   ```bash
   conda update -n base -c conda-forge conda
   ```

## Step 5: Install Required Python Packages

1. **Install conda packages:**
   ```bash
   conda install -c conda-forge mahotas numpy matplotlib pytz h5py pandas cartopy bottleneck scikit-image xarray joblib scikit-learn -y
   conda install -c conda-forge lmfit=1.1.0 -y
   ```

2. **Install pip packages:**
   ```bash
   pip install opencv-python pyephem boto3 python-dotenv s3fs pymysql sshtunnel paramiko scp PyGithub pipreqs tensorflow
   ```

## Step 6: Install and Configure Jupyter

1. **Install Jupyter:**
   ```bash
   conda install -c conda-forge jupyter jupyterlab ipython -y
   ```

2. **Create IPython startup directory:**
   ```bash
   mkdir -p ~/.ipython/profile_default/startup
   ```

3. **Add AirglowRSSS modules to Python path:**
   ```bash
   echo 'import sys; sys.path.append("'$HOME'/src/airglowrsss/src/")' > ~/.ipython/profile_default/startup/01-add-path.py
   ```

## Step 7: Install tmux

1. **Install tmux:**
   ```bash
   brew install tmux
   ```

## Step 8: Create Jupyter Auto-Start Script

1. **Create the startup script:**
   Run the following command, then paste all the lines from `#!/bin/bash` to `EOF` when prompted:
   ```bash
   cat > ~/start_jupyter.sh << 'EOF'
   ```
   Copy and paste this entire block:
   ```bash
   #!/bin/bash
   cd ~/notebooks
   source /opt/homebrew/bin/conda.sh
   conda activate airglowrsss
   jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root
   EOF
   ```

2. **Make the script executable:**
   ```bash
   chmod +x ~/start_jupyter.sh
   ```

## Step 9: Create tmux Session Management

1. **Create tmux configuration file:**
   Run the following command, then paste all the lines from `# Set default shell` to `EOF` when prompted:
   ```bash
   cat > ~/.tmux.conf << 'EOF'
   ```
   Copy and paste this entire block:
   ```bash
   # Set default shell
   set-option -g default-shell /bin/zsh

   # Enable mouse support
   set -g mouse on

   # Set status bar
   set -g status-bg black
   set -g status-fg white
   set -g status-left '#[fg=green]Session: #S #[default]'
   set -g status-right '#[fg=blue]%H:%M %d-%b-%y'

   # Pane border colors
   set -g pane-border-style fg=magenta
   set -g pane-active-border-style fg=green
   EOF
   ```

2. **Create tmux startup script:**
   Run the following command, then paste all the lines from `#!/bin/bash` to `EOF` when prompted:
   ```bash
   cat > ~/start_airglowrsss_tmux.sh << 'EOF'
   ```
   Copy and paste this entire block:
   ```bash
   #!/bin/bash

   # Check if tmux session exists
   if tmux has-session -t airglowrsss 2>/dev/null; then
       echo "AirglowRSSS tmux session already exists. Attaching..."
       tmux attach-session -t airglowrsss
   else
       echo "Creating new AirglowRSSS tmux session..."
       tmux new-session -d -s airglowrsss -c ~/notebooks
       tmux send-keys -t airglowrsss 'source /opt/homebrew/bin/conda.sh' Enter
       tmux send-keys -t airglowrsss 'conda activate airglowrsss' Enter
       tmux send-keys -t airglowrsss 'jupyter lab --ip=0.0.0.0 --port=8888 --no-browser' Enter
       
       # Create additional panes for monitoring
       tmux split-window -h -t airglowrsss
       tmux send-keys -t airglowrsss:0.1 'source /opt/homebrew/bin/conda.sh' Enter
       tmux send-keys -t airglowrsss:0.1 'conda activate airglowrsss' Enter
       tmux send-keys -t airglowrsss:0.1 'cd ~/src/airglowrsss' Enter
       
       tmux select-pane -t airglowrsss:0.0
       
       echo "Jupyter Lab started in tmux session 'airglowrsss'"
       echo "Access Jupyter at: http://localhost:8888"
       echo "To attach to session: tmux attach-session -t airglowrsss"
       echo "To detach from session: Ctrl+b then d"
       
       tmux attach-session -t airglowrsss
   fi
   EOF
   ```

3. **Make the tmux script executable:**
   ```bash
   chmod +x ~/start_airglowrsss_tmux.sh
   ```

## Step 10: Test the Setup

1. **Test the tmux Jupyter environment:**
   ```bash
   ~/start_airglowrsss_tmux.sh
   ```

2. **Verify Jupyter is running:**
   - Open a web browser and go to `http://localhost:8888`
   - If prompted for a token, look in the Terminal output for a line like:
     `http://localhost:8888/lab?token=abc123...` and copy the token part
   - **Alternative method if you can't copy from Terminal:** Open a **new Terminal window** (`Cmd+T` or `Cmd+N`) and run:
     ```bash
     conda activate airglowrsss
     jupyter lab list
     ```
     This will show the running Jupyter server with its token
   - You should see the Jupyter Lab interface

3. **Test tmux commands (performed within the tmux session):**
   - `Ctrl+b` then `d` to detach from tmux session (this is done in the Terminal window where tmux is running)
   - `tmux attach-session -t airglowrsss` to reattach (run this in a new Terminal window)
   - `tmux list-sessions` to see all sessions (run this in a new Terminal window)

## Step 11: Daily Usage

**To start your AirglowRSSS environment each time you need it:**

1. **Open Terminal** (`Cmd + Space`, type "Terminal", press Enter)

2. **Run the startup script:**
   ```bash
   ~/start_airglowrsss_tmux.sh
   ```

3. **Access Jupyter Lab:**
   - Open a web browser and go to `http://localhost:8888`
   - If prompted for a token, open a new Terminal window and run:
     ```bash
     conda activate airglowrsss
     jupyter lab list
     ```

4. **When finished working:**
   - In the tmux session, press `Ctrl+b` then `d` to detach
   - The session continues running in the background
   - To stop completely: `tmux kill-session -t airglowrsss`

## Useful Commands

- **Start tmux session:** `~/start_airglowrsss_tmux.sh`
- **List tmux sessions:** `tmux list-sessions`
- **Attach to session:** `tmux attach-session -t airglowrsss`
- **Kill session:** `tmux kill-session -t airglowrsss`
- **Activate conda environment manually:** `conda activate airglowrsss`
- **Check Jupyter status:** Look for process with `ps aux | grep jupyter`

## Troubleshooting

- If conda commands don't work, run: `source /opt/homebrew/bin/conda.sh`
- If Jupyter doesn't start, check: `conda activate airglowrsss && which jupyter`
- For tmux issues, check: `tmux info`
- For path issues, verify: `echo $PATH`