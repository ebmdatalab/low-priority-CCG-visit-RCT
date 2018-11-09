# low-priority-CCG-visit-RCT

20 CCGs will receive an invitation to have a structured feedback session delivered by a senior NHS England representative to explore their implementation of guidance on low priority prescribing.

Here is the protocol: https://docs.google.com/document/d/1rYlEG5MUgkXDvw34G8XgkWmXoelLswPVWVBWZ1o0LgU/edit


# Setup

Create a virtualenv:

    mkvirtualenv -p /usr/bin/python3 lpvisitccg
    workon lpvisitccg

Install requirements:

    pip install -r requirements


Install the virutalenv as a kernel for jupyter to use>:

    python -m ipykernel install --user --name lpvisitccg --display-name "Python (lpvisitccg)"
