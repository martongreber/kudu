# Learning tool for developers

## Intro
Run Kudu examples and have easy access to the logs and the Chromium traces!
[<img src="https://img.youtube.com/vi/m1TAMkoo-EU/hqdefault.jpg" width="1000" height="600"
/>](https://youtu.be/m1TAMkoo-EU)

## Requirements
* if OS == macOS
    * iTerm: https://iterm2.com/downloads.html
* Python: currently tested with 3.8
* Jupyter notebook: https://jupyter.org/install
* Kudu built and Python client working (basically to run the Python client example)

## Getting started
1. ```$ jupyter lab $KUDU_HOME```
2. open the $KUDU_HOME/examples/learning/kudu.ipynb
3. Run -> Restart Kernel and Run All Cells
4. Learn:
    * Find all the logs under: /tmp/cluster/...
        * on macOS the notebook will open:
            * iTerm split view with all master logs
            * iTerm split view with all the tserver logs
    * Find Chromium tracing file under: /tmp/traces
        * all the master and tserver traces are collected into one file
        * Drag and drop the tracing json file into https://ui.perfetto.dev/ to see the diagrams
            * On macOS in Finder -> Go -> Go To -> enter /tmp
            * Using the legacy Perfetto UI the ordering of the traces is preserved:
                * first 3 are the master traces
                * last 3 are the tserver traces
5. Iterate on the Python example
    * Feel free to implement own experiments, or just strip down the Python example to observer specific events e.g.: table creation, row insertion ... Don't forget to adjust the log level accordingly:
        ```
        --master-flags "--v 2" \
        --tserver-flags "--v 2"
        ```
    * Then re-run the notebook to see what is happening.


### Tech
* Just make it 'modern'/easy to use. E.g.: not everyone is that familiar with Tmux management. Therefore, standard iTerm splits are used.
* User Jupyter notebook to make it easier for everyone to extend/tune the setup.
