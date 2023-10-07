tell application "iTerm"
    activate

    -- Create a new tab
    set newTab to (create window with default profile)

    # Split pane
    tell current session of current window
        split vertically with default profile
        split vertically with default profile
    end tell

    # Exec commands
    tell first session of current tab of current window
        write text "less /tmp/cluster/tserver-0/log/kudu-tserver.INFO"
    end tell
    tell second session of current tab of current window
        write text "less /tmp/cluster/tserver-1/log/kudu-tserver.INFO"
    end tell
    tell third session of current tab of current window
        write text "less /tmp/cluster/tserver-2/log/kudu-tserver.INFO"
    end tell


end tell
tell application "iTerm"
    set zoomed of first window to true
end tell