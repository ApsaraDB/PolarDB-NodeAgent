
function PluginInit(ctx)
    a = {}
    a["a"] = ctx["ccc"]
    a["b"] = 2
    return a, true
end

function PluginRun()
    a = {}
    a["c"] = 2
    a["z"] =  1/ 0
    return a, true
end

function PluginExit()
end

