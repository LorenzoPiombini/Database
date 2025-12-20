local db = require "db"
local recs, err = db.get_all_records("test")

if not recs then
  print("ERROR:", err)
  os.exit(1)
end


for i, rec in ipairs(recs) do
print(recs[i].file_name);
  print(("==== Record #%d ===="):format(i))
  print("file_name     :", rec.file_name)
  print("file_offset   :", rec.file_offset)
  print("fields_number :", rec.fields_number)
  print("count         :", rec.count)


  print("fields:")
  if rec.fields then
    for name, value in pairs(rec.fields) do
      print("  ", name, value)
    end
  else
    print("  (no fields table)")
  end

  print()
end

