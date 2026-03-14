/* Navigation - mobile toggle + active page highlight */
(function () {
  'use strict';

  document.addEventListener('DOMContentLoaded', function () {
    // ---- Mobile hamburger ----
    var hamburger = document.getElementById('nav-hamburger');
    var mobile    = document.getElementById('nav-mobile');

    if (hamburger && mobile) {
      hamburger.addEventListener('click', function () {
        var open = mobile.classList.toggle('open');
        hamburger.setAttribute('aria-expanded', open);
      });
    }

    // ---- Active link ----
    var page = window.location.pathname.split('/').pop() || 'index.html';
    var links = document.querySelectorAll('.nav__links a, .nav__mobile a');
    links.forEach(function (a) {
      var href = a.getAttribute('href');
      if (href === page || (page === '' && href === 'index.html')) {
        a.classList.add('active');
      }
    });
  });
})();
