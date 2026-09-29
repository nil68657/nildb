// Runs before first paint so a saved light theme does not flash dark.
(function () {
  try {
    var t = localStorage.getItem('nildb-ui.theme');
    document.documentElement.dataset.theme = t === 'light' ? 'light' : 'dark';
  } catch (e) {
    document.documentElement.dataset.theme = 'dark';
  }
})();
