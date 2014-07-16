qs = require('querystring')
fs = require('fs')
_ = require('lodash')

class WeedFS

  constructor: (@config) ->
    _.defaults(@config, {
      host: 'localhost'
      port: 9333
      scheme: 'http'
    })
    @client = @config.client ? require('request')
    @address = "#{@config.scheme}://#{@config.host}:#{config.port}"
    return

  _parse: (callback) ->
    (err, response, body) ->
      if err then callback(err)
      else if /application\/json/.test(response.headers['content-type'])
        callback(null, JSON.parse(body))
      else
        try
          callback(null, JSON.parse(body))
        catch err
          callback(new Error("Unexpected content-type '#{response.headers['content-type']}' in response"))

  _assign: (options, callback) ->
    @client("#{@address}/dir/assign?#{qs.stringify(options)}", @_parse(callback))
    return

  _write: (file_url, buffer, callback) ->
    req = @client.post(file_url, @_parse((err, result) ->
      if err then return callback(err)
      callback(null, result)
    ))
    form = req.form()
    form.append("file", buffer)
    return

  clusterStatus: (callback) ->
    @client("#{@address}/dir/status", @_parse(callback))
    return

  volumeStatus: () ->

  find: (file_id, callback) ->
    [volume] = file_id.split(',')
    @client("#{@address}/dir/lookup?volumeId=#{volume}", @_parse((err, result) =>
      if (err) then return callback(err)

      locations = []

      for location in result.locations
        locations.push "#{@config.scheme}://#{location.publicUrl}/#{file_id}"

      callback(locations)
    ))
    return

  read: (file_id, stream, callback) ->

    if _.isFunction(stream)
      callback = stream
      stream = null

    @find(file_id, (locations) =>
      if locations.length > 0
        if stream?
          @client(locations[0]).pipe(stream)
        else
          @client(
            {
              method: 'GET'
              encoding: null
              url: locations[0]
            }, (err, response, body) ->
              if response.statusCode is 404
                callback(new Error("file '#{file_id}' not found"))
              else
                callback(err, response, body)
          )
      else
        callback(new Error("file '#{file_id}' not found"))
    )
    return

  write: (files, callback) ->
    if not _.isArray(files)
      files = [files]
    @_assign({ count: files.length }, (err, file_info) =>
      if (err) then return callback(err)

      is_error = false
      results = []

      _callback = (err, result) ->
        if err
          is_error = true
          results.push err
        else
          results.push result
        if results.length is files.length
          if is_error
            callback(new Error("An error occured while upload files"), file_info, results)
          else
            callback(null, file_info, results)

      for index, file of files
        file = if file instanceof Buffer then file else fs.createReadStream(file)

        if parseInt(index) is 0
          @_write("#{@config.scheme}://#{file_info.publicUrl}/#{file_info.fid}", file, _callback)
        else
          @_write("#{@config.scheme}://#{file_info.publicUrl}/#{file_info.fid}_#{index}", file, _callback)
    )
    return


module.exports = WeedFS


