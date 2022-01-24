(function () {
  const messageBox = document.querySelector('#messageBox');
  const searchForm = document.querySelector("#search-form");

  function showMessage(msg) {
    messageBox.textContent += `\n${msg}`;
    messageBox.scrollTop = messageBox.scrollHeight;
  }

  function handleResponse(response) {
    return response.ok
      ? response.json().then((data) => JSON.stringify(data, null, 2))
      : Promise.reject(new Error('Unexpected response'));
  }

  searchForm.addEventListener("submit", function (event) {
    event.preventDefault();
    const form = document.getElementById('search-form');
    const cardCondition = form.elements['cardCondition'].value;
    const cardLanguage = form.elements['cardLanguage'].value;
    const isFoiled = form.elements['isFoiled'].checked;
    const cardIdsString = form.elements['cardIds'].value
    const cardIds = cardIdsString.split(/(,| )+/).filter(id => id.trim().length > 0);

    console.log(cardIds)

    let options = {
      method: 'POST',
      headers: {accept: 'application/json', 'content-type': 'application/json'},
      credentials: 'same-origin',
      body: JSON.stringify({
          cond: cardCondition,
          lang: cardLanguage,
          foil: isFoiled,
          cards: cardIds
      }), };

    fetch('/api/best-set-seller', options)
      .then(handleResponse)
      .then(showMessage)
      .catch(function (err) {
        showMessage(err.message);
      });

  });
})();
