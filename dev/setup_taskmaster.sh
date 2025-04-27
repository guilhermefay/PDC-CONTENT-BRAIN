#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# --- Configuration ---
PNPM_GLOBAL_DIR="$HOME/.pnpm-global"
# Force using .zshrc based on user's default shell info
SHELL_CONFIG_FILE="$HOME/.zshrc"
echo "Targeting shell config file: $SHELL_CONFIG_FILE (based on system default)"

# --- PNPM Environment Setup ---
echo "Ensuring PNPM environment is set up..."

# Create PNPM global directory if it doesn't exist
mkdir -p "$PNPM_GLOBAL_DIR"
echo "PNPM global directory ensured at: $PNPM_GLOBAL_DIR"

# Check and add PNPM_HOME to shell config if missing
PNPM_EXPORT_LINE="export PNPM_HOME=\"$PNPM_GLOBAL_DIR\""
if ! grep -qF "$PNPM_EXPORT_LINE" "$SHELL_CONFIG_FILE" 2>/dev/null; then
    echo "$PNPM_EXPORT_LINE" >> "$SHELL_CONFIG_FILE"
    echo "Added PNPM_HOME export to $SHELL_CONFIG_FILE"
else
    echo "PNPM_HOME export already present in $SHELL_CONFIG_FILE"
fi

# Check and add PNPM_HOME to PATH in shell config if missing
# More robust check for PATH modification using case statement
PATH_CONFIG_BLOCK_START="# Add PNPM_HOME to PATH if not already present"
if ! grep -qF "$PATH_CONFIG_BLOCK_START" "$SHELL_CONFIG_FILE" 2>/dev/null; then
    # Add PATH export using a structure that avoids duplicates if script runs multiple times
    echo "" >> "$SHELL_CONFIG_FILE" # Add a newline for separation
    echo "$PATH_CONFIG_BLOCK_START" >> "$SHELL_CONFIG_FILE"
    echo "if [ -d \"\$PNPM_HOME\" ]; then" >> "$SHELL_CONFIG_FILE"
    echo "  case \":\$PATH:\" in" >> "$SHELL_CONFIG_FILE"
    echo "    *\":\$PNPM_HOME:\"* ) ;;" >> "$SHELL_CONFIG_FILE" # Already in PATH
    echo "    *) export PATH=\"\$PNPM_HOME:\$PATH\" ;;" >> "$SHELL_CONFIG_FILE" # Add to PATH
    echo "  esac" >> "$SHELL_CONFIG_FILE"
    echo "fi" >> "$SHELL_CONFIG_FILE"
    echo "Added PNPM_HOME to PATH configuration in $SHELL_CONFIG_FILE"
else
    echo "PNPM_HOME PATH configuration block already present in $SHELL_CONFIG_FILE"
fi

# Export for the current script execution (might not help verification step, but good practice)
export PNPM_HOME="$PNPM_GLOBAL_DIR"
export PATH="$PNPM_HOME:$PATH"
echo "Exported PNPM_HOME and updated PATH for current script session."

# --- Taskmaster CLI Installation ---
echo "Installing/Updating task-master-ai CLI..."
INSTALL_CMD=""
if command -v pnpm >/dev/null 2>&1; then
    INSTALL_CMD="pnpm add -g task-master-ai"
elif command -v npm >/dev/null 2>&1; then
    INSTALL_CMD="npm i -g task-master-ai"
else
     echo "Error: Neither pnpm nor npm found. Please install one." >&2
     exit 1
fi

echo "Attempting installation with: $INSTALL_CMD"
if $INSTALL_CMD; then
    echo "Installation command successful."
else
    echo "Error: Failed to install task-master-ai." >&2
    # Try the other one if the first failed (e.g., pnpm failed, try npm)
    if [[ "$INSTALL_CMD" == *"pnpm"* ]] && command -v npm >/dev/null 2>&1; then
        echo "Trying installation with npm as fallback..."
        if npm i -g task-master-ai; then
             echo "npm fallback installation successful."
        else
             echo "Error: Fallback npm installation also failed." >&2
             exit 1
        fi
    elif [[ "$INSTALL_CMD" == *"npm"* ]] && command -v pnpm >/dev/null 2>&1; then
         echo "Trying installation with pnpm as fallback..."
         if pnpm add -g task-master-ai; then
             echo "pnpm fallback installation successful."
         else
             echo "Error: Fallback pnpm installation also failed." >&2
             exit 1
         fi
    else
        exit 1 # Exit if primary failed and no fallback available/tried
    fi
fi


# --- Verification ---
# This verification might still fail within the script's execution context
# The crucial test is manual verification by the user after sourcing .zshrc
echo "Verifying taskmaster installation (within script context)..."
# Try running with the explicit path first
EXPECTED_BIN_PATH="$PNPM_GLOBAL_DIR/taskmaster"
ALT_BIN_PATH="$(npm prefix -g)/bin/taskmaster" # Get potential npm path

if [ -x "$EXPECTED_BIN_PATH" ] && "$EXPECTED_BIN_PATH" --version; then
    VERSION=$("$EXPECTED_BIN_PATH" --version)
    echo -e "\n\e[32m✅ Environment appears ready (verified via direct path). Taskmaster CLI version: $VERSION\e[0m"
    echo "Please run 'source $SHELL_CONFIG_FILE' or restart your terminal for the command to be globally available via PATH."
elif [ -x "$ALT_BIN_PATH" ] && "$ALT_BIN_PATH" --version; then
    VERSION=$("$ALT_BIN_PATH" --version)
    echo -e "\n\e[32m✅ Environment appears ready (verified via direct npm path). Taskmaster CLI version: $VERSION\e[0m"
    echo "Please run 'source $SHELL_CONFIG_FILE' or restart your terminal for the command to be globally available via PATH."
else
     echo "Warning: 'taskmaster --version' command might still fail in this script's context." >&2
     echo "Please run 'source $SHELL_CONFIG_FILE' or restart your terminal, then manually run 'taskmaster --version' to confirm."
     # Exit non-zero to indicate potential issue for automation, but allow manual continuation
     exit 1
fi

exit 0 