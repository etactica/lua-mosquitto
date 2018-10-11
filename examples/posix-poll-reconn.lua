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
    needs_reconn = 0,
}

mosq.init()
local mqtt = mosq.new(cfg.MOSQ_CLIENT_ID, true)

local function mqtt_reconnector()
    local ok, code, err = mqtt:connect_async(cfg.HOST, 1883, 60)
    if not ok then
        print(string.format("Failed to make MQTT connection: %d:%s", code, err))
        cfg.needs_reconn = cfg.needs_reconn + 1
        return {}
    end
    return { [mqtt:socket()] = {events={ IN=true, OUT=true } }, }
end

local function handle_data(topic, payload)
    print(string.format("Received data on topic %s -> %s", topic, payload))
end

local function handle_command(topic, payload)
    print(string.format("Received command on topic %s -> %s", topic, payload))
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
end

mqtt.ON_MESSAGE = function(mid, topic, payload, qos, retain)
    if mosq.topic_matches_sub(cfg.TOPIC_DATA, topic) then
        return handle_data(topic, payload)
    end
    if mosq.topic_matches_sub(cfg.TOPIC_COMMAND, topic) then
        return handle_command(topic, payload)
    end
end

local fds = mqtt_reconnector()

while true do
    local x = P.poll(fds, cfg.MOSQ_IDLE_LOOP_MS)
    local res, code, nok
    if x > 0 then
        for fd in pairs(fds) do
            if fds[fd].revents.IN then
                res, code, nok = mqtt:loop_read()
                if not res then
                    cfg.needs_reconn = cfg.needs_reconn + 1
                    print(string.format("MQTT loop read failed returned %d:%s", code, nok))
                else
                    cfg.needs_reconn = 0
                end
            end
            if fds[fd].revents.OUT then
                res, code, nok = mqtt:loop_write()
                if not res then
                    cfg.needs_reconn = cfg.needs_reconn + 1
                    print(string.format("MQTT loop write failed returned %d:%s", code, nok))
                else
                    cfg.needs_reconn = 0
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
        print(string.format("MQTT Misc loop faiure: %d:%s", code, nok))
    end
    if fds[mqtt:socket()] then
        fds[mqtt:socket()].events.OUT = mqtt:want_write()
    end

    if not mqtt:socket() or cfg.needs_reconn > 0 then
        print(string.format("MQTT connection lost, reconnecting count: %d", cfg.needs_reconn))
        fds = mqtt_reconnector()
    end
end
