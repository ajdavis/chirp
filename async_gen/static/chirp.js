$(function() {
    var $form                   = $('#new_chirp'),
        $text_input             = $('#text_input'),
        $new_chirp_success      = $('#new_chirp_success'),
        socket                  = io.connect();

    // TODO: organize
    var Chirp = Backbone.Model.extend({
        defaults: {
            ts:  'none',
            msg: 'none'
        }
    });

    var ChirpCollection = Backbone.Collection.extend({
        model: Chirp,
        url:   '/chirps'
    });

    var chirps = new ChirpCollection();

    var ChirpView = Backbone.View.extend({
        tagName: 'li',
        template: _.template($('#chirp-template').html()),
        initialize: function() {
            this.model.bind('destroy', this.remove, this);
        },
        render: function() {
            $(this.el).html(this.template(this.model.toJSON()));
            return this;
        }
    });

    var ChirpsView = Backbone.View.extend({
        initialize: function() {
            chirps.bind('add',   this.addOne, this);
            chirps.bind('reset', this.addAll, this);
            chirps.fetch({cache: false});
        },
        addOne: function(chirp) {
            var view = new ChirpView({model: chirp});
            $("#chirp-list").prepend(view.render().el);
        },
        addAll: function() {
            $('#chirp-list').empty();
            chirps.each(this.addOne);
        }
    });

    var chirps_view = new ChirpsView();

    socket.on('connect',    function()    { socket.emit('get_chirps');  });
    socket.on('cleared',    function()    { chirps.reset();             });
    socket.on('app_error',  function(msg) { alert(msg);                 });
    socket.on('disconnect', function()    { socket = io.connect();      });

    socket.on('chirps', function (json) {
        var data = JSON.parse(json);
        if (data.chirps) {
            _(data.chirps).each(function(chirp) {
                chirps.add(new Chirp(chirp));
            });
        }
    });

    $form.submit(function() {
        var msg = $text_input.val();
        $.ajax({
            type: 'POST',
            url:  '/new',
            data: msg,

            success: function(data, textStatus, jqXHR) {
                $text_input.val('');
                $new_chirp_success.fadeIn(
                    // Callback to fade the success indicator out once
                    // it's completed fading in
                    function() { $new_chirp_success.fadeOut(); }
                );
            },

            error: function(jqXHR, textStatus, errorThrown) {
                $new_chirp_success.hide();

                alert("Error saving chirp: " + jqXHR.responseText);
            }
        });

        return false;
    });

    $('#clear_chirps').click(function() {
        if (confirm('Really clear the collection?')) {
            $.ajax({
                type: 'POST',
                url: '/clear',

                success: function() {
                    $('#capped_coll').empty();
                },

                error: function(jqXHR, textStatus, errorThrown) {
                    var msg = jqXHR.responseText || textStatus;
                    if (msg) {
                        alert("Error clearing capped_coll: " + msg);
                    }
                }
            });
        }
    });
});
