const AWS = require('aws-sdk')
const s3 = new AWS.S3()
const ses = new AWS.SES()
const dynamodb = new AWS.DynamoDB.DocumentClient()

console.log('AWS Lambda SES Forwarder // @arithmetric // Version 5.1.0')

// Configuración del forwarder de correos basado en AWS SES.
//
// Esta función Lambda:
// 1. Recibe correos entrantes desde SES.
// 2. Determina los destinatarios finales según reglas de mapeo.
// 3. Verifica si el remitente está en la blacklist (DynamoDB).
// 4. Si NO está bloqueado:
//    - Recupera el correo desde S3
//    - Ajusta headers (From, Reply-To, Subject, etc.)
//    - Reenvía el correo mediante SES
// 5. Si está en blacklist:
//    - Detiene el flujo y no reenvía el correo
//
// Parámetros de configuración:
//
// - fromEmail:
//   Dirección verificada en SES utilizada como base para el envío de correos.
//   Puede ser sobrescrita dinámicamente durante el procesamiento.
//
// - subjectPrefix:
//   Prefijo opcional que se añade al Subject del correo reenviado.
//
// - emailBucket:
//   Nombre del bucket S3 donde SES almacena los correos entrantes.
//
// - emailKeyPrefix:
//   Prefijo del path dentro del bucket donde se guardan los correos.
//   Debe incluir la barra final si aplica.
//
// - allowPlusSign:
//   Permite normalizar direcciones que usan "+" en el alias.
//   Ejemplo: example+test@dominio.com → example@dominio.com
//
// - forwardMapping:
//   Reglas de redirección de correos.
//   Define cómo se transforman los destinatarios originales en nuevos destinatarios.
//
//   Formato:
//   {
//     "origen": ["destino1", "destino2"]
//   }
//
//   Reglas soportadas:
//
//   - Email completo:
//     "usuario@dominio.com"
//     Coincidencia exacta.
//
//   - Dominio completo:
//     "@dominio.com"
//     Coincide con cualquier usuario de ese dominio.
//
//   - Usuario (sin dominio):
//     "info"
//     Coincide con ese usuario en cualquier dominio.
//
//   - Default:
//     "@"
//     Aplica si no hay ninguna otra coincidencia.
//
// Notas importantes:
//
// - Todas las comparaciones de correos se realizan en minúsculas.
// - El sistema depende de S3 para obtener el contenido del correo.
// - La blacklist se consulta en DynamoDB usando el email del remitente.
// - Si el remitente está bloqueado (enabled = true), el correo no se procesa.
// - El sistema no elimina correos, solo controla su reenvío.

var defaultConfig = {
  fromEmail: 'portal@infomovil.com.bo',
  subjectPrefix: '',
  emailBucket: 'infomovil-ses-mails',
  emailKeyPrefix: '',
  allowPlusSign: true,
  forwardMapping: {
    'ricardo.ascarrunz@infomovil.com.bo': ['rickymarti2013@gmail.com'],
    '@infomovil.com.bo': ['hector@geosoft.com.bo'],
    '@geosoft.website': ['hector@geosoft.com.bo']
  }
}

function normalizeEmail(email) {
  return (email || '').trim().toLowerCase()
}

function extractHeaderValue(emailData, headerName) {
  const regex = new RegExp(
    '^' + headerName + ':[\\t ]?(.*(?:\\r?\\n\\s+.*)*)',
    'im'
  )
  const match = emailData.match(regex)

  if (!match || !match[1]) return ''

  return match[1].replace(/\r?\n\s+/g, ' ').trim()
}

function extractEmail(value) {
  const match = value.match(/<([^>]+)>/)
  return normalizeEmail(match ? match[1] : value)
}

/**
 * Parses the SES event record provided for the `mail` and `receipients` data.
 *
 * @param {object} data - Data bundle with context, email, etc.
 *
 * @return {object} - Promise resolved with data.
 */
exports.parseEvent = function (data) {
  // Validate characteristics of a SES event record.
  if (
    !data.event ||
    !data.event.hasOwnProperty('Records') ||
    data.event.Records.length !== 1 ||
    !data.event.Records[0].hasOwnProperty('eventSource') ||
    data.event.Records[0].eventSource !== 'aws:ses' ||
    data.event.Records[0].eventVersion !== '1.0'
  ) {
    data.log({
      message: 'parseEvent() received invalid SES message:',
      level: 'error',
      event: JSON.stringify(data.event)
    })
    return Promise.reject(new Error('Error: Received invalid SES message.'))
  }

  data.email = data.event.Records[0].ses.mail
  data.recipients = data.event.Records[0].ses.receipt.recipients
  return Promise.resolve(data)
}

/**
 * Transforms the original recipients to the desired forwarded destinations.
 *
 * @param {object} data - Data bundle with context, email, etc.
 *
 * @return {object} - Promise resolved with data.
 */
exports.transformRecipients = function (data) {
  var newRecipients = []
  data.originalRecipients = data.recipients
  data.recipients.forEach(function (origEmail) {
    var origEmailKey = origEmail.toLowerCase()
    if (data.config.allowPlusSign) {
      origEmailKey = origEmailKey.replace(/\+.*?@/, '@')
    }
    if (data.config.forwardMapping.hasOwnProperty(origEmailKey)) {
      newRecipients = newRecipients.concat(
        data.config.forwardMapping[origEmailKey]
      )
      data.originalRecipient = origEmail
    } else {
      var origEmailDomain
      var origEmailUser
      var pos = origEmailKey.lastIndexOf('@')
      if (pos === -1) {
        origEmailUser = origEmailKey
      } else {
        origEmailDomain = origEmailKey.slice(pos)
        origEmailUser = origEmailKey.slice(0, pos)
      }
      if (
        origEmailDomain &&
        data.config.forwardMapping.hasOwnProperty(origEmailDomain)
      ) {
        newRecipients = newRecipients.concat(
          data.config.forwardMapping[origEmailDomain]
        )
        data.originalRecipient = origEmail
      } else if (
        origEmailUser &&
        data.config.forwardMapping.hasOwnProperty(origEmailUser)
      ) {
        newRecipients = newRecipients.concat(
          data.config.forwardMapping[origEmailUser]
        )
        data.originalRecipient = origEmail
      } else if (data.config.forwardMapping.hasOwnProperty('@')) {
        newRecipients = newRecipients.concat(data.config.forwardMapping['@'])
        data.originalRecipient = origEmail
      }
    }
  })

  if (!newRecipients.length) {
    data.log({
      message:
        'Finishing process. No new recipients found for ' +
        'original destinations: ' +
        data.originalRecipients.join(', '),
      level: 'info'
    })
    return data.callback()
  }

  data.recipients = newRecipients
  return Promise.resolve(data)
}

/**
 * Verifica si el remitente está en la blacklist de DynamoDB.
 *
 * @param {object} data - Data bundle con context, email, etc.
 *
 * @return {object} - Promise resuelta con data.
 */
exports.checkBlacklist = function (data) {
  const tableName = process.env.BLACKLIST_TABLE_NAME

  if (!tableName) {
    data.log({
      level: 'warn',
      message:
        'BLACKLIST_TABLE_NAME no está configurada. Se omite validación de blacklist.'
    })
    return Promise.resolve(data)
  }

  const candidates = []

  const source = normalizeEmail(data.email.source)
  if (source) candidates.push({ type: 'source', email: source })

  if (data.emailData) {
    const replyTo = extractEmail(extractHeaderValue(data.emailData, 'Reply-To'))
    if (replyTo) candidates.push({ type: 'reply-to', email: replyTo })
  }

  data.log({
    level: 'info',
    message:
      'Consultando blacklist para: ' +
      candidates.map((c) => c.type + '=' + c.email).join(', ')
  })

  return new Promise(function (resolve, reject) {
    function checkNext(index) {
      if (index >= candidates.length) {
        data.log({
          level: 'info',
          message:
            'Ninguna dirección está bloqueada. Continúa el procesamiento.'
        })
        return resolve(data)
      }

      const candidate = candidates[index]

      dynamodb.get(
        {
          TableName: tableName,
          Key: {
            email: candidate.email
          }
        },
        function (err, result) {
          if (err) {
            data.log({
              level: 'error',
              message: 'Error consultando DynamoDB blacklist.',
              error: err,
              stack: err.stack
            })
            return reject(
              new Error('Error: No se pudo consultar la blacklist.')
            )
          }

          if (result.Item && result.Item.enabled === true) {
            data.log({
              level: 'warn',
              message: 'Correo bloqueado por blacklist.',
              blockedBy: candidate.type,
              email: candidate.email,
              reason: result.Item.reason || null,
              messageId: data.email.messageId
            })

            return data.callback()
          }

          return checkNext(index + 1)
        }
      )
    }

    checkNext(0)
  })
}

/**
 * Fetches the message data from S3.
 *
 * @param {object} data - Data bundle with context, email, etc.
 *
 * @return {object} - Promise resolved with data.
 */
exports.fetchMessage = function (data) {
  // Copying email object to ensure read permission
  data.log({
    level: 'info',
    message:
      'Fetching email at s3://' +
      data.config.emailBucket +
      '/' +
      data.config.emailKeyPrefix +
      data.email.messageId
  })
  return new Promise(function (resolve, reject) {
    data.s3.copyObject(
      {
        Bucket: data.config.emailBucket,
        CopySource:
          data.config.emailBucket +
          '/' +
          data.config.emailKeyPrefix +
          data.email.messageId,
        Key: data.config.emailKeyPrefix + data.email.messageId,
        ACL: 'private',
        ContentType: 'text/plain',
        StorageClass: 'STANDARD'
      },
      function (err) {
        if (err) {
          data.log({
            level: 'error',
            message: 'copyObject() returned error:',
            error: err,
            stack: err.stack
          })
          return reject(
            new Error('Error: Could not make readable copy of email.')
          )
        }

        // Load the raw email from S3
        data.s3.getObject(
          {
            Bucket: data.config.emailBucket,
            Key: data.config.emailKeyPrefix + data.email.messageId
          },
          function (err, result) {
            if (err) {
              data.log({
                level: 'error',
                message: 'getObject() returned error:',
                error: err,
                stack: err.stack
              })
              return reject(
                new Error('Error: Failed to load message body from S3.')
              )
            }
            data.emailData = result.Body.toString()
            return resolve(data)
          }
        )
      }
    )
  })
}

/**
 * Processes the message data, making updates to recipients and other headers
 * before forwarding message.
 *
 * @param {object} data - Data bundle with context, email, etc.
 *
 * @return {object} - Promise resolved with data.
 */
exports.processMessage = function (data) {
  var match = data.emailData.match(/^((?:.+\r?\n)*)(\r?\n(?:.*\s+)*)/m)
  var header = match && match[1] ? match[1] : data.emailData
  var body = match && match[2] ? match[2] : ''

  // Add "Reply-To:" with the "From" address if it doesn't already exists
  if (!/^reply-to:[\t ]?/im.test(header)) {
    match = header.match(/^from:[\t ]?(.*(?:\r?\n\s+.*)*\r?\n)/im)
    var from = match && match[1] ? match[1] : ''
    if (from) {
      header = header + 'Reply-To: ' + from
      data.log({
        level: 'info',
        message: 'Added Reply-To address of: ' + from
      })
    } else {
      data.log({
        level: 'info',
        message:
          'Reply-To address not added because From address was not ' +
          'properly extracted.'
      })
    }
  }

  // SES does not allow sending messages from an unverified address,
  // so replace the message's "From:" header with the original
  // recipient (which is a verified domain)
  header = header.replace(
    /^from:[\t ]?(.*(?:\r?\n\s+.*)*)/gim,
    function (match, from) {
      var fromText
      if (data.config.fromEmail) {
        fromText =
          'From: ' +
          from.replace(/<(.*)>/, '').trim() +
          ' <' +
          data.config.fromEmail +
          '>'
      } else {
        fromText =
          'From: ' +
          from.replace('<', 'at ').replace('>', '') +
          ' <' +
          data.originalRecipient +
          '>'
      }
      return fromText
    }
  )

  // Add a prefix to the Subject
  if (data.config.subjectPrefix) {
    header = header.replace(
      /^subject:[\t ]?(.*)/gim,
      function (match, subject) {
        return 'Subject: ' + data.config.subjectPrefix + subject
      }
    )
  }

  // Replace original 'To' header with a manually defined one
  if (data.config.toEmail) {
    header = header.replace(/^to:[\t ]?(.*)/gim, 'To: ' + data.config.toEmail)
  }

  // Remove the Return-Path header.
  header = header.replace(/^return-path:[\t ]?(.*)\r?\n/gim, '')

  // Remove Sender header.
  header = header.replace(/^sender:[\t ]?(.*)\r?\n/gim, '')

  // Remove Message-ID header.
  header = header.replace(/^message-id:[\t ]?(.*)\r?\n/gim, '')

  // Remove all DKIM-Signature headers to prevent triggering an
  // "InvalidParameterValue: Duplicate header 'DKIM-Signature'" error.
  // These signatures will likely be invalid anyways, since the From
  // header was modified.
  header = header.replace(/^dkim-signature:[\t ]?.*\r?\n(\s+.*\r?\n)*/gim, '')

  data.emailData = header + body
  return Promise.resolve(data)
}

/**
 * Send email using the SES sendRawEmail command.
 *
 * @param {object} data - Data bundle with context, email, etc.
 *
 * @return {object} - Promise resolved with data.
 */
exports.sendMessage = function (data) {
  // Aseguramos que se use siempre el originalRecipient como remitente
  data.config.fromEmail = data.originalRecipient

  var params = {
    Destinations: data.recipients,
    Source: data.config.fromEmail,
    RawMessage: {
      Data: data.emailData
    }
  }
  data.log({
    level: 'info',
    message:
      'sendMessage: Sending email via SES. Original recipients: ' +
      data.originalRecipients.join(', ') +
      '. Transformed recipients: ' +
      data.recipients.join(', ') +
      '.'
  })
  return new Promise(function (resolve, reject) {
    data.ses.sendRawEmail(params, function (err, result) {
      if (err) {
        data.log({
          level: 'error',
          message: 'sendRawEmail() returned error.',
          error: err,
          stack: err.stack
        })
        return reject(new Error('Error: Email sending failed.'))
      }
      data.log({
        level: 'info',
        message: 'sendRawEmail() successful.',
        result: result
      })
      resolve(data)
    })
  })
}

/**
 * Handler function to be invoked by AWS Lambda with an inbound SES email as
 * the event.
 *
 * @param {object} event - Lambda event from inbound email received by AWS SES.
 * @param {object} context - Lambda context object.
 * @param {object} callback - Lambda callback object.
 * @param {object} overrides - Overrides for the default data, including the
 * configuration, SES object, and S3 object.
 */
exports.handler = function (event, context, callback, overrides) {
  var steps =
    overrides && overrides.steps
      ? overrides.steps
      : [
          exports.parseEvent,
          exports.transformRecipients,
          exports.fetchMessage,
          exports.checkBlacklist,
          exports.processMessage,
          exports.sendMessage
        ]
  var data = {
    event: event,
    callback: callback,
    context: context,
    config: overrides && overrides.config ? overrides.config : defaultConfig,
    log: overrides && overrides.log ? overrides.log : console.log,
    ses: overrides && overrides.ses ? overrides.ses : new AWS.SES(),
    s3:
      overrides && overrides.s3
        ? overrides.s3
        : new AWS.S3({ signatureVersion: 'v4' })
  }
  Promise.series(steps, data)
    .then(function (data) {
      data.log({
        level: 'info',
        message: 'Process finished successfully.'
      })
      return data.callback()
    })
    .catch(function (err) {
      data.log({
        level: 'error',
        message: 'Step returned error: ' + err.message,
        error: err,
        stack: err.stack
      })
      return data.callback(new Error('Error: Step returned error.'))
    })
}

Promise.series = function (promises, initValue) {
  return promises.reduce(function (chain, promise) {
    if (typeof promise !== 'function') {
      return chain.then(() => {
        throw new Error('Error: Invalid promise item: ' + promise)
      })
    }
    return chain.then(promise)
  }, Promise.resolve(initValue))
}
