from ldn.cli_grid import list_countries

ldn_countries = ['Fiji', 'Saint Lucia', 'Saint Kitts and Nevis', 'Mauritius', 'Antigua and Barbuda', 'Cook Islands', 'Vanuatu', 'Martinique', 'American Samoa', 'Cuba', 'Puerto Rico', 'Seychelles', 'Curaçao', 'Turks and Caicos Islands', 'Dominican Republic', 'Grenada', 'Saint Vincent and the Grenadines', 'Bahamas', 'Anguilla', 'Guyana', 'Micronesia', 'Guadeloupe', 'Barbados', 'Montserrat', 'Dominica', 'Samoa', 'French Polynesia', 'Northern Mariana Islands', 'British Virgin Islands', 'Aruba', 'Sint Maarten', 'Bermuda', 'Jamaica', 'Tonga', 'U.S. Virgin Islands', 'Wallis and Futuna', 'Tuvalu', 'Palau', 'Suriname', 'Guinea-Bissau', 'Papua New Guinea', 'Haiti', 'Cape Verde', 'Marshall Islands', 'Comoros', 'New Caledonia', 'Cayman Islands', 'Trinidad and Tobago', 'Belize', 'São Tomé and Príncipe', 'Guam', 'Solomon Islands', 'Tokelau', 'Timor-Leste', 'Pitcairn Islands', 'Singapore', 'Maldives', 'Niue', 'Kiribati', 'Nauru']

def test_list_countries() -> None:
    countries_sorted = list_countries().sort()
    ldn_countries_sorted = ldn_countries.sort()
    assert countries_sorted == ldn_countries_sorted
