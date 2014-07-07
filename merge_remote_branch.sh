#!/bin/bash

# Checks arguments
if [ -z "$1" ]
  then
    echo -e "\e[1;31mError: No argument for the repository supplied.\e[0m Usage: merge_remote_branch.sh reposity_name branch_name" && exit
fi
if [ -z "$2" ]
  then
    echo -e "\e[1;31mError: No argument for the branch supplied.\e[0m Usage: merge_remote_branch.sh reposity_name branch_name" && exit
fi

# Verifies presence of remotes
remote_check=`git remote | grep ethz-asl_$1`
if [ -z "$remote_check" ]
  then
    git remote add -f ethz-asl_$1 https://github.com/ethz-asl/$1.git
fi

branch_name=$(git symbolic-ref -q HEAD)
branch_name=${branch_name##refs/heads/}
branch_name=${branch_name:-HEAD}

echo -e "\e[1;31mThis will merge \"ethz-asl_$1 $2\" into your local branch \"$branch_name\"! \e[0m"

while true; do
    read -p "Continue? [y/n]" yn
    case $yn in
        [Yy]* ) git fetch ethz-asl_$1 $2 && git subtree pull --prefix $1 ethz-asl_$1 $2; break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
done



