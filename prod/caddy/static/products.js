function esc(s) {
    var d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
}

fetch('/products.yaml')
    .then(function(r) {
        if (!r.ok) {
            throw new Error('products.yaml returned HTTP ' + r.status);
        }
        return r.text();
    })
    .then(function(text) {
        return jsyaml.load(text);
    })
    .then(function(products) {
        var container = document.getElementById('cards');

        if (!Array.isArray(products)) {
            throw new Error('products.yaml must contain a product list');
        }

        container.innerHTML = products.map(function(p) {
            var slug = esc(p.slug).replace(/[^a-z0-9-]/gi, '');
            return '<div class="card">' +
                '<div class="card-header">' +
                    '<h3>' + esc(p.name) + '</h3>' +
                    (p.category ? '<span class="category">' + esc(p.category) + '</span>' : '') +
                '</div>' +
                '<p>' + esc(p.description) + '</p>' +
                '<a href="/' + slug + '/">Open</a>' +
            '</div>';
        }).join('');
    })
    .catch(function(err) {
        document.getElementById('cards').innerHTML =
            '<p class="error">Failed to load products: ' + esc(err.message) + '</p>';
    });
