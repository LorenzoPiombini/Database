
function rec_to_json(rec)
	local f = rec.fields
	local json = "{ "
	for key, value in pairs(f) do
		local sb
		if type(value) == "string" then
			sb = string.format('"%s":"%s",', key, value)
		elseif type(value) == "number" then
			if math.type(value) == "integer" then
				sb = string.format('"%s":"%d",', key, value)
			else
				sb = string.format('"%s":"%.2f",', key, value)
			end
		end
		json = string.format("%s%s", json, sb)
	end
	json = string.sub(json, 1, #json - 1)
	json = string.format("%s%s", json, "}")
	return json
end
