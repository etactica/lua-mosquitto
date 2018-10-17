#!/usr/bin/lua
--[[
Demonstrates using Posix poll() to connect and reconnect to a mosquitto server
Karl Palsson, 2018 <karlp@etactica.com>
]]

local mosq = require("mosquitto")
local P = require("posix")

local cfg = {
    MOSQ_CLIENT_ID = string.format("myApp-%d", P.unistd.getpid()),
    MOSQ_IDLE_LOOP_MS = 500,
    TOPIC_DATA = "data/#",
    TOPIC_COMMAND = "command/#",
    HOST = "localhost",
    must_exit = false,
    needs_reconn = 0,
}

mosq.init()
local mqtt = mosq.new(cfg.MOSQ_CLIENT_ID, true)

local function mqtt_reconnector()
    print("current reconn iteration = ", cfg.needs_reconn)
    local function calculate_backoff(basetime, iter)
        if iter < 5 then return basetime * 2 end
        if iter < 10 then return basetime * 4 end
        return basetime * 8
    end

    local ok, code, err = mqtt:connect(cfg.HOST, 1883, 60)
    if not ok then
        print(string.format("Failed to make MQTT connection: %d:%s", code, err))
        cfg.needs_reconn = cfg.needs_reconn + 1
        return {}, calculate_backoff(cfg.MOSQ_IDLE_LOOP_MS, cfg.needs_reconn)
    end
    return { [mqtt:socket()] = {events={ IN=true, OUT=true } }, }, cfg.MOSQ_IDLE_LOOP_MS
end

local function handle_data(topic, payload)
    print(string.format("Received data on topic %s -> %s", topic, payload))
end

local function handle_command(topic, payload)
    print(string.format("Received command on topic %s -> %s", topic, payload))
    if topic:find("quit") then cfg.must_exit = true end
end

mqtt.ON_CONNECT = function()
    local mid, code, nok
    mid, code, nok = mqtt:subscribe(cfg.TOPIC_DATA, 0)
    if not mid then
        error(string.format("Aborting, unable to subscribe to data topic: %d:%s", code, nok))
    end
    mid, code, nok = mqtt:subscribe(cfg.TOPIC_COMMAND, 0)
    if not mid then
        error(string.format("Aborting, unable to subscribe to command topic: %d:%s", code, nok))
    end
    print(string.format("MQTT (RE)Connected happily to %s", cfg.HOST))
    cfg.needs_reconn = 0
end

mqtt.ON_DISCONNECT = function(code)
    print("mosquitto disconnect with code", code)
    cfg.needs_reconn = cfg.needs_reconn + 1
end

mqtt.ON_MESSAGE = function(mid, topic, payload, qos, retain)
    if mosq.topic_matches_sub(cfg.TOPIC_DATA, topic) then
        return handle_data(topic, payload)
    end
    if mosq.topic_matches_sub(cfg.TOPIC_COMMAND, topic) then
        return handle_command(topic, payload)
    end
end


local fds, backoff_time
local function mainloop()
    local x = P.poll(fds, backoff_time)
    local res, code, nok
    if x > 0 then
        for fd in pairs(fds) do
            if fds[fd].revents.IN then
                res, code, nok = mqtt:loop_read()
                if not res then
                    print(string.format("MQTT loop read failed returned %d:%s", code, nok))
                    fds, backoff_time = mqtt_reconnector()
                    return
                end
            end
            if fds[fd].revents.OUT then
                res, code, nok = mqtt:loop_write()
                if not res then
                    print(string.format("MQTT loop write failed returned %d:%s", code, nok))
                    fds, backoff_time = mqtt_reconnector()
                    return
                end
            end
        end
    end
    if x < 0 then
        error("Poll returned negative! %d %d %s", x, P.errno())
    end

    res, code, nok = mqtt:loop_misc()
    if not res then
        -- This is largely redundant with the loop read/write error handling.
        print(string.format("MQTT Misc loop failure: %d:%s", code, nok))
        fds, backoff_time = mqtt_reconnector()
        return
    end
    -- Should always be valid at this point
    fds[mqtt:socket()].events.OUT = mqtt:want_write()

end

fds, backoff_time = mqtt_reconnector()
while not cfg.must_exit do
    mainloop()
end
