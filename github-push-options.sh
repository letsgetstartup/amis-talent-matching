#!/bin/bash

echo "=== ALTERNATIVE GITHUB SETUP ==="
echo ""

echo "OPTION 1: Use Personal Access Token"
echo "1. Go to: https://github.com/settings/tokens"
echo "2. Click 'Generate new token (classic)'"
echo "3. Give it a name: 'AMIS Project'"
echo "4. Select scope: 'repo' (full control)"
echo "5. Click Generate"
echo "6. Copy the token"
echo ""

echo "Then run these commands:"
echo "git config --global credential.helper store"
echo "git push -u origin main"
echo "# When prompted, enter:"
echo "# Username: letsgetstartup"
echo "# Password: [paste your token]"
echo ""

echo "OPTION 2: Use SSH (if you have SSH keys)"
echo "git remote remove origin"
echo "git remote add origin git@github.com:letsgetstartup/amis-talent-matching.git"
echo "git push -u origin main"
echo ""

echo "OPTION 3: Try GitHub CLI again"
echo "gh auth login --web"
echo ""

echo "Choose whichever option works for you!"
