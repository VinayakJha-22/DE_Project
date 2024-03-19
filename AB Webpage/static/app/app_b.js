var page = 'B'
var event_buy_now = false
var event_buy = false
var event_buy_three = false
var event_buy_six = false
var event_buy_nine = false
var event_auth = false
var event_location = ''

document.getElementById('buy-now-b').addEventListener('click', function() {
    document.getElementById('product-b').scrollIntoView({ behavior: 'smooth' });
  });

  document.getElementById('buy-now-3').addEventListener('click', function() {
    event_buy = true
    event_buy_three = true
    document.getElementById('buy-now-3').innerText = 'item bought'
  });
document.getElementById('buy-now-6').addEventListener('click', function() {
    event_buy = true
    event_buy_six = true
    document.getElementById('buy-now-6').innerText = 'item bought'
  });
document.getElementById('buy-now-9').addEventListener('click', function() {
    event_buy = true
    event_buy_nine = true
    document.getElementById('buy-now-9').innerText = 'item bought'
  });
document.getElementById('auth-b').addEventListener('click', function() {
    event_auth = true
    document.getElementById('auth-b').innerText = "Hi"
  });
  

var cities = ['delhi', 'lucknow', 'chennai', 'mathura', 'vrindavan', 'kolkata', 'mumbai', 'pune', 'bangalore'];
function getRandomCity() {
  var randomIndex = Math.floor(Math.random() * cities.length); // Generate a random index
  return cities[randomIndex]; // Return the city at the random index
}
event_location = getRandomCity();

function generateUniqueId() {
  const timestamp = Date.now().toString(36); // Convert current timestamp to base36 string
  const randomNum = Math.random().toString(36).substr(2, 5); // Generate random number and convert to base36 string
  return timestamp + randomNum; // Combine timestamp and random number
}

const visit_id = generateUniqueId();

const currentDate = new Date();
const year = currentDate.getFullYear();
const month = String(currentDate.getMonth() + 1).padStart(2, '0'); // Months are zero-based
const day = String(currentDate.getDate()).padStart(2, '0');
const capture_date = `${year}-${month}-${day}`;


window.addEventListener('beforeunload', function(event) {
  data = {
    'page': page,
    'visit_id': visit_id,
    'event_auth': event_auth,
    'event_buynow': event_buy_now,
    'event_buy': event_buy,
    'event_buy_three': event_buy_three,
    'event_buy_six': event_buy_six,
    'event_buy_nine': event_buy_nine,
    'event_location': event_location,
    'capture_date': capture_date
  }

  const api_serve_data = JSON.stringify(data);
});