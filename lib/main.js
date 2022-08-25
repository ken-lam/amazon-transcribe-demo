const audioUtils        = require('./audioUtils');  // for encoding audio data as PCM
const crypto            = require('crypto'); // tot sign our pre-signed URL
const v4                = require('./aws-signature-v4'); // to generate our pre-signed URL
const marshaller        = require("@aws-sdk/eventstream-marshaller"); // for converting binary event stream messages to and from JSON
const util_utf8_node    = require("@aws-sdk/util-utf8-node"); // utilities for encoding and decoding UTF8
const mic               = require('microphone-stream'); // collect microphone input as a stream of raw bytes

// our converter between binary event streams messages and JSON
const eventStreamMarshaller = new marshaller.EventStreamMarshaller(util_utf8_node.toUtf8, util_utf8_node.fromUtf8);

// our global variables for managing state
let languageCode;
let region;
let sampleRate;
let transcription = "";
let translation = "";
let transalted;
let socket;
let micStream;
let socketError = false;
let transcribeException = false;
let accessKeyId
let secretAccessKey
let sessionToken

AWS.config.region = 'ap-southeast-2';
region = AWS.config.region
var AWSPoolID = 'ap-southeast-2:6df56912-934f-4f5e-ae51-c9208e464639';

AWS.config.credentials = new AWS.CognitoIdentityCredentials({
    IdentityPoolId: AWSPoolID
});
AWS.config.credentials.get(function(err) {
    if (err) alert(err);
    //console.log(AWS.config.credentials);
    accessKeyId = AWS.config.credentials.accessKeyId
    secretAccessKey = AWS.config.credentials.secretAccessKey
    sessionToken = AWS.config.credentials.sessionToken
});

// check to see if the browser allows mic access
if (!window.navigator.mediaDevices.getUserMedia) {
    // Use our helper method to show an error on the page
    showError('We support the latest versions of Chrome, Firefox, Safari, and Edge. Update your browser and try your request again.');

    // maintain enabled/distabled state for the start and stop buttons
    toggleStartStop();
}

$('#start-button').click(function () {
    $('#error').hide(); // hide any existing errors
    toggleStartStop(true); // disable start and enable stop button

    // set the language from the dropdowns
    setLanguage();
    formatTransalte();

    // first we get the microphone input from the browser (as a promise)...
    window.navigator.mediaDevices.getUserMedia({
            video: false,
            audio: true
        })
        // ...then we convert the mic stream to binary event stream messages when the promise resolves
        .then(streamAudioToWebSocket)
        .catch(function (error) {
            showError('There was an error streaming your audio to Amazon Transcribe. Please try again. ' + error);
            toggleStartStop();
        });
});

let streamAudioToWebSocket = function (userMediaStream) {
    //let's get the mic input from the browser, via the microphone-stream module
    micStream = new mic();
    micStream.setStream(userMediaStream);

    // Pre-signed URLs are a way to authenticate a request (or WebSocket connection, in this case)
    // via Query Parameters. Learn more: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
    let url = createPresignedUrl();

    //open up our WebSocket connection
    socket = new WebSocket(url);
    socket.binaryType = "arraybuffer";

    // when we get audio data from the mic, send it to the WebSocket if possible
    socket.onopen = function() {
        micStream.on('data', function(rawAudioChunk) {
            // the audio stream is raw audio bytes. Transcribe expects PCM with additional metadata, encoded as binary
            let binary = convertAudioToBinaryMessage(rawAudioChunk);

            if (socket.OPEN)
                socket.send(binary);
        }
    )};

    // handle messages, errors, and close events
    wireSocketEvents();
}

function wireSocketEvents() {
    // handle inbound messages from Amazon Transcribe
    socket.onmessage = function (message) {
        //convert the binary event stream message to JSON
        let messageWrapper = eventStreamMarshaller.unmarshall(Buffer(message.data));
        let messageBody = JSON.parse(String.fromCharCode.apply(String, messageWrapper.body));
        if (messageWrapper.headers[":message-type"].value === "event") {
            handleEventStreamMessage(messageBody);
        }
        else {
            transcribeException = true;
            showError(messageBody.Message);
            toggleStartStop();
        }
    };

    socket.onerror = function () {
        socketError = true;
        showError('WebSocket connection error. Try again.');
        toggleStartStop();
    };

    socket.onclose = function (closeEvent) {
        micStream.stop();

        // the close event immediately follows the error event; only handle one.
        if (!socketError && !transcribeException) {
            if (closeEvent.code != 1000) {
                showError('</i><strong>Streaming Exception</strong><br>' + closeEvent.reason);
            }
            toggleStartStop();
        }
    };
}

function setLanguage() {
    languageCode = $('#language').find(':selected').val();
    if (languageCode == "en-US" || languageCode == "es-US")
        sampleRate = 44100;
    else
        sampleRate = 44100;
}

function disableLanguage() {
  var currentLanguage;
  currentLanguage = $('#language').find(':selected').val().substring(0,2);
  console.log(currentLanguage);
  $('#translateTo option').each(function(index,element){
    if (currentLanguage == element.value){
      $(this).prop("disabled", true);
    }
    else {
      $(this).prop("disabled", false);
    }
  });
}

function formatTransalte(){
  var currentLanguage = $('#translateTo').find(':selected').val();
  if (currentLanguage == 'he' || currentLanguage == 'ar') {
    $('#translate').css('direction','rtl');
  }
  else {
    $('#translate').css('direction','ltr');
  }
}

let handleEventStreamMessage = function (messageJson) {
    let results = messageJson.Transcript.Results;
    if (results.length > 0) {
        if (results[0].Alternatives.length > 0) {
            console.log(results);

            let transcript = results[0].Alternatives[0].Transcript;

            // fix encoding for accented characters
            transcript = decodeURIComponent(escape(transcript));
            // update the textarea with the latest result
            $('#transcript').val(transcription + transcript + "\n");
            $('#transcript').scrollTop($('#transcript')[0].scrollHeight);


            // if this transcript segment is final, add it to the overall transcription
            if (!results[0].IsPartial) {
                //scroll the textarea down
                $('#transcript').scrollTop($('#transcript')[0].scrollHeight);
                transcription += transcript + "\n";

                translateInput(transcript, function(transalted){
                      console.log(transalted);
                      translation += transalted + "\n";
                      //$('#translate').val(translation + "\n");
                      $('#translate').val(translation + "\n");
                      $('#translate').scrollTop($('#translate')[0].scrollHeight);
                    }
                )
            }
        }
    }
}

let closeSocket = function () {
    if (socket.OPEN) {
        micStream.stop();

        // Send an empty frame so that Transcribe initiates a closure of the WebSocket after submitting all transcripts
        let emptyMessage = getAudioEventMessage(Buffer.from(new Buffer([])));
        let emptyBuffer = eventStreamMarshaller.marshall(emptyMessage);
        socket.send(emptyBuffer);
    }
}

function translateInput(text, callback) {
  var source_language = document.getElementById('source_lang');
  // var source_language = 'auto'
  var target_language = $('#translateTo').find(':selected').val();

  var urlParams = new URLSearchParams(window.location.search);
  if (urlParams.has('language')) {
    var target_language = urlParams.get('language')
  }

  var translate = new AWS.Translate();
  var params = {
    SourceLanguageCode: source_language,
    TargetLanguageCode: target_language,
    Text: text
  };
  translate.translateText(params, function (err, data) {
    console.log(data);
    callback(data.TranslatedText);
  });
}

$('#stop-button').click(function () {
    closeSocket();
    toggleStartStop();
});

$('#reset-button').click(function (){
    $('#transcript').val('');
    transcription = '';
    $('#translate').val('');
    translation = '';
});

$('#language').change(function (){
    disableLanguage();
});

$('#translateTo').change(function (){
    formatTransalte();
});

function toggleStartStop(disableStart = false) {
    $('#start-button').prop('disabled', disableStart);
    $('#stop-button').attr("disabled", !disableStart);
}

function showError(message) {
    $('#error').html('<i class="fa fa-times-circle"></i> ' + 'Demo Only - ' + message);
    $('#error').show();
}

$("#transcript").change(function() {
  scrollToBottom();
});

function scrollToBottom() {
  $('#transcript').scrollTop($('#transcript')[0].scrollHeight);
}

$("#translate").change(function() {
  scrollToBottom2();
});

function scrollToBottom2() {
  $('#translate').scrollTop($('#translate')[0].scrollHeight);
}

function convertAudioToBinaryMessage(audioChunk) {
    let raw = mic.toRaw(audioChunk);

    if (raw == null)
        return;

    // downsample and convert the raw audio bytes to PCM
    let downsampledBuffer = audioUtils.downsampleBuffer(raw, sampleRate);
    let pcmEncodedBuffer = audioUtils.pcmEncode(downsampledBuffer);

    // add the right JSON headers and structure to the message
    let audioEventMessage = getAudioEventMessage(Buffer.from(pcmEncodedBuffer));

    //convert the JSON object + headers into a binary event stream message
    let binary = eventStreamMarshaller.marshall(audioEventMessage);

    return binary;
}

function getAudioEventMessage(buffer) {
    // wrap the audio data in a JSON envelope
    return {
        headers: {
            ':message-type': {
                type: 'string',
                value: 'event'
            },
            ':event-type': {
                type: 'string',
                value: 'AudioEvent'
            }
        },
        body: buffer
    };
}

function createPresignedUrl() {
    let endpoint = "transcribestreaming." + region + ".amazonaws.com:8443";
    console.log(accessKeyId);
    console.log(sessionToken);
    // get a preauthenticated URL that we can use to establish our WebSocket
    return v4.createPresignedURL(
        'GET',
        endpoint,
        '/stream-transcription-websocket',
        'transcribe',
        crypto.createHash('sha256').update('', 'utf8').digest('hex'), {
            'key': accessKeyId,
            'secret': secretAccessKey,
            'sessionToken': sessionToken,
            'protocol': 'wss',
            'expires': 15,
            'region': region,
            'query': "language-code=" + languageCode + "&media-encoding=pcm&sample-rate=" + sampleRate
        }
    );
}
