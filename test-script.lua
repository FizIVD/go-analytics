local json = require "cjson"

function random_string(len)
    local res = {}
    for i = 1, len do
        res[i] = string.char(math.random(97, 122))
    end
    return table.concat(res)
end

function random_payload()
    return {
        device_metadata = {
            os = random_string(5),
            version = tostring(math.random(1, 20)),
            model = random_string(6)
        },
        profile_data = {
            user_id = math.random(1, 20000),
            region = random_string(3)
        },
        event_data = {
            action = random_string(4),
            value = math.random(1, 1000)
        }
    }
end

request = function()
    local body = json.encode(random_payload())
    return wrk.format("POST", "/event", {["Content-Type"] = "application/json"}, body)
end
