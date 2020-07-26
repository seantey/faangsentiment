$.getJSON("data/test_7_days.json", function(sentiment_data) {
    console.log(sentiment_data)

    const array = ["FB", "AMZN", "AAPL", "NFLX", "GOOG"]
    array.forEach(function (t_symb, index) {
        console.log(t_symb);

        for (var i = 0; i < 7; i++) {
            let day = i+1

            // Generate Selector String for each daily box with Template Strings
            box_selector = `#${t_symb}-box-${day}`
            box_text_selector = `#${t_symb}-box-${day} > p`

            // Load the JSON data for the current day and t_symb
            let day_key_str = `day-${day}`
            current_data = sentiment_data[t_symb][day_key_str]

            // Modify the daily box elements
            current_box_text = document.querySelector(box_text_selector)
            current_box_text.innerHTML = current_data['month_day']
            
            current_box = document.querySelector(box_selector)
            current_box.style.backgroundColor = current_data['hex_color']

        }

        current_symbol_data = sentiment_data[t_symb]
        // Modify the current day sentiment label
        current_day_selector = `#${t_symb}-sentiment-today`
        current_day_label = document.querySelector(current_day_selector)
        current_day_label.innerHTML = current_symbol_data['day-1']['sentiment_label']
        current_day_label.style.color = current_symbol_data['day-1']['hex_color']

        // Mofify the last updated timestamp
        last_updated_selector = `#${t_symb}-timestamp`
        current_last_updated = document.querySelector(last_updated_selector)
        current_last_updated.innerHTML = current_symbol_data['day-1']['last_updated']

    });


});

