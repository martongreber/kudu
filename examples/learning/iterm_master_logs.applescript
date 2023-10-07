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
        write text "less /tmp/cluster/master-0/log/kudu-master.INFO"
    end tell
    tell second session of current tab of current window
        write text "less /tmp/cluster/master-1/log/kudu-master.INFO"
    end tell
    tell third session of current tab of current window
        write text "less /tmp/cluster/master-2/log/kudu-master.INFO"
    end tell

end tell

tell application "iTerm"
    set zoomed of first window to true
end tell