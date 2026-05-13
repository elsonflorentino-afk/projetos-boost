#!/usr/bin/env python3
"""
Gera o E-book 4: "Stablecoins: Dolarização de Portfólio"
Design system idêntico aos E-books 1 e 2. Conteúdo do MD revisado por @vera, @nina, @juri.
"""

import os
import sys

# WeasyPrint requires this on macOS
os.environ.setdefault("DYLD_LIBRARY_PATH", "/opt/homebrew/lib")

try:
    from weasyprint import HTML
except ImportError:
    print("ERRO: WeasyPrint não instalado. Rode: pip install weasyprint")
    sys.exit(1)

OUTPUT_DIR = "/Users/user/Downloads"
OUTPUT_HTML = os.path.join(OUTPUT_DIR, "EBOOK_4_STABLECOINS_v1.html")
OUTPUT_PDF = os.path.join(OUTPUT_DIR, "EBOOK_4_STABLECOINS_v1.pdf")

# ── Watermark SVG (Boost logo in green, very subtle) ─────────────────────
import urllib.parse as _urlparse

_WATERMARK_SVG = '''<svg xmlns="http://www.w3.org/2000/svg" width="921" height="249" viewBox="0 0 921 249" fill="none"><path fill-rule="evenodd" clip-rule="evenodd" d="M0.258436 1.064C-0.103564 1.65-0.0825625 30.562 0.304437 65.314C1.08544 135.51 0.780437 132.635 7.83244 136.302C14.0304 139.525 16.9234 138.383 44.4764 121.836C58.5214 113.401 70.2254 105.967 70.4874 105.315C70.7484 104.663 69.5314 101.13 67.7814 97.464C66.0314 93.798 64.9824 90.439 65.4494 90.001C65.9164 89.562 79.0524 87.605 94.6414 85.652C119.596 82.526 123.72 81.755 129.141 79.203C145.237 71.627 154.691 55.82 153.571 38.36C152.652 24.046 143.201 11.342 127.702 3.588L120.529 0H60.7234C19.5164 0 0.711436 0.331 0.258436 1.064ZM140.619 85.36C139.893 86.108 129.623 99.137 117.798 114.314C105.973 129.491 96.0734 141.925 95.7984 141.945C95.5234 141.966 93.5814 139.317 91.4814 136.058C89.3824 132.8 87.1144 129.677 86.4414 129.118C85.5574 128.385 73.6074 135.061 43.4524 153.135C20.4814 166.902 1.83044 178.596 2.00544 179.12C2.19844 179.7 23.6944 180.306 56.8104 180.665C117.355 181.321 117.618 181.3 130.798 174.737C140.242 170.034 151.445 158.663 156.155 149C162.56 135.857 163.192 119.608 157.885 104.508C155.66 98.176 153.095 94.185 147.434 88.25C142.968 83.566 142.539 83.384 140.619 85.36Z" fill="#00B37E"/><path d="M267.746 248.5V219.36H278.652C282.343 219.36 285.243 220.248 287.352 222.025C289.489 223.773 290.558 226.215 290.558 229.351C290.558 231.405 290.072 233.181 289.101 234.679C288.157 236.15 286.797 237.288 285.021 238.093C283.245 238.87 281.122 239.259 278.652 239.259H269.452L270.826 237.843V248.5H267.746ZM287.644 248.5L280.151 237.926H283.481L291.016 248.5H287.644ZM270.826 238.093L269.452 236.636H278.569C281.483 236.636 283.689 235.998 285.188 234.721C286.714 233.444 287.477 231.654 287.477 229.351C287.477 227.02 286.714 225.216 285.188 223.939C283.689 222.663 281.483 222.025 278.569 222.025H269.452L270.826 220.568V238.093ZM323.222 248.5V219.36H343.203V222.025H326.302V245.836H343.828V248.5H323.222ZM325.969 235.012V232.39H341.371V235.012H325.969ZM384.496 248.75C382.331 248.75 380.25 248.403 378.252 247.709C376.281 246.988 374.755 246.072 373.673 244.962L374.88 242.589C375.907 243.588 377.28 244.434 379.001 245.128C380.749 245.794 382.581 246.127 384.496 246.127C386.328 246.127 387.812 245.905 388.95 245.461C390.116 244.989 390.962 244.365 391.489 243.588C392.045 242.811 392.322 241.951 392.322 241.007C392.322 239.869 391.989 238.953 391.323 238.259C390.685 237.566 389.838 237.025 388.784 236.636C387.729 236.22 386.563 235.859 385.287 235.554C384.01 235.248 382.734 234.929 381.457 234.596C380.18 234.235 379.001 233.764 377.919 233.181C376.864 232.598 376.004 231.835 375.338 230.891C374.699 229.92 374.38 228.657 374.38 227.103C374.38 225.66 374.755 224.342 375.504 223.148C376.281 221.927 377.461 220.956 379.043 220.234C380.625 219.485 382.65 219.111 385.12 219.111C386.758 219.111 388.381 219.346 389.991 219.818C391.6 220.262 392.988 220.887 394.154 221.691L393.113 224.148C391.864 223.315 390.532 222.704 389.117 222.316C387.729 221.927 386.383 221.733 385.079 221.733C383.33 221.733 381.887 221.969 380.749 222.441C379.612 222.913 378.765 223.551 378.21 224.356C377.683 225.133 377.419 226.021 377.419 227.02C377.419 228.158 377.738 229.074 378.377 229.767C379.043 230.461 379.903 231.002 380.958 231.391C382.04 231.779 383.219 232.126 384.496 232.432C385.773 232.737 387.035 233.07 388.284 233.431C389.561 233.791 390.726 234.263 391.781 234.846C392.863 235.401 393.724 236.15 394.362 237.094C395.028 238.037 395.361 239.272 395.361 240.799C395.361 242.214 394.972 243.532 394.195 244.753C393.418 245.947 392.225 246.918 390.615 247.667C389.033 248.389 386.994 248.75 384.496 248.75ZM427.466 248.5V219.36H447.447V222.025H430.546V245.836H448.072V248.5H427.466ZM430.213 235.012V232.39H445.616V235.012H430.213ZM476.044 248.5L489.365 219.36H492.404L505.725 248.5H502.478L490.239 221.15H491.488L479.249 248.5H476.044ZM481.289 240.716L482.205 238.218H499.147L500.063 240.716H481.289ZM535.938 248.5V219.36H546.845C550.536 219.36 553.436 220.248 555.545 222.025C557.682 223.773 558.75 226.215 558.75 229.351C558.75 231.405 558.265 233.181 557.293 234.679C556.35 236.15 554.99 237.288 553.214 238.093C551.438 238.87 549.315 239.259 546.845 239.259H537.645L539.019 237.843V248.5H535.938ZM555.836 248.5L548.343 237.926H551.674L559.208 248.5H555.836ZM539.019 238.093L537.645 236.636H546.761C549.675 236.636 551.882 235.998 553.38 234.721C554.907 233.444 555.67 231.654 555.67 229.351C555.67 227.02 554.907 225.216 553.38 223.939C551.882 222.663 549.675 222.025 546.761 222.025H537.645L539.019 220.568V238.093ZM604.069 248.75C601.877 248.75 599.851 248.389 597.991 247.667C596.132 246.918 594.522 245.877 593.163 244.545C591.803 243.213 590.734 241.645 589.957 239.841C589.208 238.037 588.833 236.067 588.833 233.93C588.833 231.793 589.208 229.823 589.957 228.019C590.734 226.215 591.803 224.647 593.163 223.315C594.55 221.983 596.174 220.956 598.033 220.234C599.892 219.485 601.918 219.111 604.111 219.111C606.22 219.111 608.204 219.471 610.064 220.193C611.923 220.887 613.491 221.941 614.768 223.357L612.811 225.313C611.618 224.092 610.299 223.218 608.856 222.691C607.413 222.136 605.859 221.858 604.194 221.858C602.446 221.858 600.822 222.163 599.323 222.774C597.825 223.357 596.521 224.203 595.41 225.313C594.3 226.395 593.426 227.672 592.788 229.143C592.177 230.586 591.872 232.182 591.872 233.93C591.872 235.679 592.177 237.288 592.788 238.759C593.426 240.202 594.3 241.479 595.41 242.589C596.521 243.671 597.825 244.518 599.323 245.128C600.822 245.711 602.446 246.002 604.194 246.002C605.859 246.002 607.413 245.725 608.856 245.17C610.299 244.615 611.618 243.727 612.811 242.506L614.768 244.462C613.491 245.877 611.923 246.946 610.064 247.667C608.204 248.389 606.206 248.75 604.069 248.75ZM667.635 248.5V219.36H670.673V248.5H667.635ZM646.321 248.5V219.36H649.401V248.5H646.321ZM649.068 235.054V232.348H667.926V235.054H649.068Z" fill="#00B37E"/><path d="M810.705 179V50.2034H759.635V4.61395H920.568V50.2034H869.498V179H810.705Z" fill="#00B37E"/><path d="M676.562 182.986C661.947 182.986 647.83 181.325 634.211 178.003C620.593 174.682 609.382 170.364 600.58 165.049L619.513 122.2C627.817 127.016 637.035 130.919 647.166 133.909C657.463 136.732 667.428 138.144 677.06 138.144C682.707 138.144 687.108 137.812 690.264 137.147C693.586 136.317 695.994 135.237 697.489 133.909C698.983 132.414 699.731 130.67 699.731 128.677C699.731 125.522 697.987 123.03 694.499 121.203C691.011 119.377 686.361 117.882 680.548 116.719C674.901 115.391 668.673 114.062 661.864 112.733C655.055 111.239 648.162 109.329 641.187 107.003C634.377 104.678 628.066 101.606 622.253 97.786C616.607 93.966 612.039 88.984 608.552 82.839C605.064 76.527 603.32 68.722 603.32 59.421C603.32 48.626 606.31 38.827 612.289 30.025C618.434 21.056 627.485 13.915 639.443 8.6C651.567 3.285 666.597 0.628 684.534 0.628C696.326 0.628 707.952 1.874 719.411 4.365C730.871 6.856 741.168 10.676 750.303 15.825L732.615 58.425C723.979 54.106 715.591 50.868 707.453 48.709C699.482 46.55 691.676 45.47 684.036 45.47C678.389 45.47 673.905 45.968 670.583 46.965C667.262 47.961 664.853 49.29 663.359 50.951C662.03 52.612 661.366 54.439 661.366 56.432C661.366 59.421 663.11 61.829 666.597 63.656C670.085 65.317 674.652 66.729 680.299 67.891C686.112 69.054 692.423 70.299 699.232 71.628C706.208 72.957 713.1 74.784 719.91 77.109C726.719 79.434 732.947 82.506 738.594 86.326C744.407 90.146 749.057 95.129 752.545 101.274C756.032 107.419 757.776 115.058 757.776 124.193C757.776 134.822 754.704 144.621 748.559 153.589C742.58 162.392 733.611 169.533 721.653 175.014C709.696 180.329 694.665 182.986 676.562 182.986Z" fill="#00B37E"/><path d="M491.799 182.986C477.682 182.986 464.561 180.744 452.437 176.26C440.479 171.775 430.099 165.464 421.297 157.326C412.495 149.022 405.602 139.306 400.62 128.179C395.803 117.051 393.395 104.927 393.395 91.807C393.395 78.52 395.803 66.396 400.62 55.435C405.602 44.308 412.495 34.675 421.297 26.537C430.099 18.233 440.479 11.839 452.437 7.354C464.561 2.87 477.682 0.628 491.799 0.628C506.082 0.628 519.202 2.87 531.16 7.354C543.118 11.839 553.498 18.233 562.3 26.537C571.103 34.675 577.912 44.308 582.729 55.435C587.711 66.396 590.202 78.52 590.202 91.807C590.202 104.927 587.711 117.051 582.729 128.179C577.912 139.306 571.103 149.022 562.3 157.326C553.498 165.464 543.118 171.775 531.16 176.26C519.202 180.744 506.082 182.986 491.799 182.986ZM491.799 135.403C497.279 135.403 502.345 134.407 506.995 132.414C511.812 130.421 515.964 127.598 519.451 123.944C523.105 120.124 525.929 115.557 527.922 110.242C529.914 104.761 530.911 98.616 530.911 91.807C530.911 84.998 529.914 78.936 527.922 73.621C525.929 68.14 523.105 63.573 519.451 59.919C515.964 56.099 511.812 53.193 506.995 51.2C502.345 49.207 497.279 48.211 491.799 48.211C486.318 48.211 481.169 49.207 476.353 51.2C471.703 53.193 467.551 56.099 463.897 59.919C460.409 63.573 457.669 68.14 455.676 73.621C453.683 78.936 452.686 84.998 452.686 91.807C452.686 98.616 453.683 104.761 455.676 110.242C457.669 115.557 460.409 120.124 463.897 123.944C467.551 127.598 471.703 130.421 476.353 132.414C481.169 134.407 486.318 135.403 491.799 135.403Z" fill="#00B37E"/><path d="M280.628 182.986C266.511 182.986 253.391 180.744 241.267 176.26C229.309 171.775 218.929 165.464 210.126 157.326C201.324 149.022 194.432 139.306 189.449 128.179C184.633 117.051 182.225 104.927 182.225 91.807C182.225 78.52 184.633 66.396 189.449 55.435C194.432 44.308 201.324 34.675 210.126 26.537C218.929 18.233 229.309 11.839 241.267 7.354C253.391 2.87 266.511 0.628 280.628 0.628C294.911 0.628 308.032 2.87 319.99 7.354C331.947 11.839 342.328 18.233 351.13 26.537C359.932 34.675 366.742 44.308 371.558 55.435C376.54 66.396 379.032 78.52 379.032 91.807C379.032 104.927 376.54 117.051 371.558 128.179C366.742 139.306 359.932 149.022 351.13 157.326C342.328 165.464 331.947 171.775 319.99 176.26C308.032 180.744 294.911 182.986 280.628 182.986ZM280.628 135.403C286.109 135.403 291.174 134.407 295.825 132.414C300.641 130.421 304.793 127.598 308.281 123.944C311.935 120.124 314.758 115.557 316.751 110.242C318.744 104.761 319.74 98.616 319.74 91.807C319.74 84.998 318.744 78.936 316.751 73.621C314.758 68.14 311.935 63.573 308.281 59.919C304.793 56.099 300.641 53.193 295.825 51.2C291.174 49.207 286.109 48.211 280.628 48.211C275.147 48.211 269.999 49.207 265.183 51.2C260.532 53.193 256.38 56.099 252.726 59.919C249.239 63.573 246.498 68.14 244.505 73.621C242.512 78.936 241.516 84.998 241.516 91.807C241.516 98.616 242.512 104.761 244.505 110.242C246.498 115.557 249.239 120.124 252.726 123.944C256.38 127.598 260.532 130.421 265.183 132.414C269.999 134.407 275.147 135.403 280.628 135.403Z" fill="#00B37E"/></svg>'''

# Make fills very subtle (fill-opacity controls watermark visibility)
_WATERMARK_SVG = _WATERMARK_SVG.replace('fill="#00B37E"', 'fill="#00B37E" fill-opacity="0.009"')
_WATERMARK_URI = "data:image/svg+xml," + _urlparse.quote(_WATERMARK_SVG, safe='')

# ── CSS ──────────────────────────────────────────────────────────────────
CSS = """
@page {
    size: A4;
    margin: 2cm 2cm 3.2cm 2cm;
    @top-right {
        content: "Boost Research";
        font-family: 'Helvetica Neue', Arial, sans-serif;
        font-size: 9pt;
        color: #666;
        border-bottom: 2px solid #00B37E;
        padding-bottom: 4px;
    }
    @bottom-center {
        content: counter(page);
        font-family: 'Helvetica Neue', Arial, sans-serif;
        font-size: 9pt;
        color: #999;
    }
    @bottom-right {
        content: "";
        font-family: 'Helvetica Neue', Arial, sans-serif;
        font-size: 7pt;
        color: #bbb;
    }
    @bottom-left {
        content: "";
    }
}

@page :first {
    margin: 0;
    @top-right { content: none; }
    @bottom-center { content: none; }
    @bottom-right { content: none; }
    @bottom-left { content: none; }
}

@page cover {
    margin: 0;
    background: #000;
    @top-right { content: none; }
    @bottom-center { content: none; }
    @bottom-right { content: none; }
    @bottom-left { content: none; }
}

* {
    box-sizing: border-box;
}

body {
    font-family: 'Helvetica Neue', Arial, sans-serif;
    font-size: 10.5pt;
    line-height: 1.55;
    color: #1A1A2E;
    background: #fff;
    orphans: 3;
    widows: 3;
}
p {
    margin-top: 0;
    margin-bottom: 8px;
    text-align: justify;
    hyphens: auto;
}

/* ── Watermark (applied via Python below) ── */

/* ── Capa ── */
.cover {
    page: cover;
    background: #000;
    color: #fff;
    position: relative;
    width: 210mm;
    height: 297mm;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    text-align: center;
    padding: 3cm;
    page-break-after: always;
    margin: 0;
}
.cover .logo-img {
    width: 280px;
    margin-bottom: 40px;
}
.cover .divider {
    width: 60px;
    height: 4px;
    background: #00B37E;
    margin: 0 auto 30px;
}
.cover h1 {
    font-size: 28pt;
    font-weight: 800;
    line-height: 1.2;
    margin-bottom: 20px;
    color: #fff;
}
.cover .subtitle {
    font-size: 13pt;
    color: #00B37E;
    margin-bottom: 40px;
    line-height: 1.5;
}
.cover .author {
    font-size: 12pt;
    color: #C9A84C;
    margin-bottom: 5px;
}
.cover .company {
    font-size: 10pt;
    color: #888;
}
.cover .year {
    font-size: 10pt;
    color: #666;
    margin-top: 5px;
}
.cover .disclaimer-cover {
    font-size: 7.5pt;
    color: #555;
    margin-top: 80px;
    max-width: 500px;
}

/* ── Sumário ── */
.toc {
    page-break-after: always;
}
.toc h2 {
    font-size: 22pt;
    color: #1A1A2E;
    border-bottom: 3px solid #00B37E;
    padding-bottom: 8px;
    display: inline-block;
    margin-bottom: 30px;
}
.toc-entry {
    display: flex;
    align-items: baseline;
    padding: 12px 0;
    border-bottom: 1px solid #eee;
}
.toc-num {
    font-size: 16pt;
    font-weight: 700;
    color: #00B37E;
    min-width: 50px;
}
.toc-title {
    font-size: 11pt;
    color: #333;
}

/* ── Títulos ── */
.chapter-label {
    font-size: 11pt;
    font-weight: 700;
    color: #00B37E;
    letter-spacing: 3px;
    text-transform: uppercase;
    margin-bottom: 5px;
}

h1 {
    font-size: 22pt;
    font-weight: 800;
    color: #1A1A2E;
    margin-top: 0;
    margin-bottom: 6px;
    border-bottom: 3px solid #00B37E;
    padding-bottom: 6px;
    display: inline-block;
}
h2 {
    font-size: 15pt;
    font-weight: 700;
    color: #1A1A2E;
    margin-top: 18px;
    margin-bottom: 6px;
    page-break-after: avoid;
}
h3 {
    font-size: 12pt;
    font-weight: 700;
    color: #1A1A2E;
    margin-top: 14px;
    margin-bottom: 5px;
    page-break-after: avoid;
}
h4 {
    font-size: 10.5pt;
    font-weight: 700;
    color: #1A1A2E;
    margin-top: 10px;
    margin-bottom: 4px;
    page-break-after: avoid;
}

/* ── Blocos especiais ── */
.learn-box {
    background: #f8f9fa;
    border-left: 4px solid #00B37E;
    padding: 10px 16px;
    margin: 10px 0 14px;
    page-break-inside: avoid;
}
.learn-box strong {
    display: block;
    margin-bottom: 8px;
    color: #1A1A2E;
}
.learn-box ul {
    margin: 0;
    padding-left: 18px;
}
.learn-box li {
    margin-bottom: 4px;
    color: #333;
}

.risk-box {
    background: #FFF8E1;
    border: 1px solid #FFD54F;
    border-left: 4px solid #FF9800;
    padding: 10px 16px;
    margin: 10px 0;
    border-radius: 4px;
    page-break-inside: avoid;
}
.risk-box strong {
    color: #E65100;
}

.boost-opinion {
    background: #E8F5E9;
    border-left: 4px solid #00B37E;
    padding: 10px 16px;
    margin: 10px 0;
    page-break-inside: avoid;
}
.boost-opinion strong {
    color: #0B6E4F;
}

.checklist-box {
    background: #f0f7f4;
    border: 1px solid #c8e6d8;
    padding: 10px 16px;
    margin: 14px 0 0;
    border-radius: 4px;
    page-break-inside: avoid;
}
.checklist-box h4 {
    margin-top: 0;
    color: #0B6E4F;
}
.checklist-box li {
    margin-bottom: 4px;
}

.nota-metodologica {
    font-size: 9pt;
    color: #666;
    font-style: italic;
    line-height: 1.4;
    margin: 10px 0;
}

/* ── Tabelas ── */
table {
    width: 100%;
    border-collapse: collapse;
    margin: 10px 0;
    font-size: 9.5pt;
    page-break-inside: avoid;
}
thead th {
    background: #00B37E;
    color: #fff;
    padding: 7px 10px;
    text-align: left;
    font-weight: 600;
}
tbody td {
    padding: 6px 10px;
    border-bottom: 1px solid #e0e0e0;
}
tbody tr:nth-child(even) {
    background: #f8f9fa;
}

/* ── Bullets ── */
ul {
    padding-left: 20px;
}
ul li {
    margin-bottom: 6px;
}
ul li::marker {
    color: #00B37E;
}

/* ── CTA final ── */
.cta-page {
    page-break-before: always;
    background: #000;
    color: #fff;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    text-align: center;
    padding: 4cm;
}
.cta-page h2 {
    color: #fff;
    font-size: 22pt;
    margin-bottom: 15px;
}
.cta-page p {
    color: #ccc;
    font-size: 12pt;
    max-width: 400px;
    margin-bottom: 25px;
}
.cta-btn {
    display: inline-block;
    background: #25D366;
    color: #fff;
    padding: 18px 40px;
    border-radius: 50px;
    font-size: 14pt;
    font-weight: 700;
    text-decoration: none;
    letter-spacing: 1px;
}
.cta-sub {
    font-size: 9pt;
    color: #888;
    margin-top: 15px;
}

/* ── Legal ── */
.legal {
    page-break-before: always;
    padding-top: 60px;
}
.legal h2 {
    border-bottom: 3px solid #00B37E;
    padding-bottom: 8px;
    display: inline-block;
}
.legal p {
    font-size: 10pt;
    color: #555;
    line-height: 1.6;
}

/* ── Page breaks ── */
.chapter {
    page-break-before: always;
}

/* ── Social footer fixo (todas as páginas) ── */
.social-footer {
    position: fixed;
    bottom: -26mm;
    left: 0;
    width: 100%;
    text-align: center;
    font-size: 7.5pt;
    color: #888;
    z-index: 100;
}
.social-footer a {
    color: #555;
    text-decoration: none;
    margin: 0 8px;
    white-space: nowrap;
}
.social-footer a:hover {
    color: #00B37E;
}
.social-footer svg {
    width: 12px;
    height: 12px;
    vertical-align: middle;
    margin-right: 3px;
}

/* ── Erros fatais ── */
.erros-fatais {
    background: #FFEBEE;
    border: 1px solid #EF9A9A;
    border-left: 4px solid #D32F2F;
    padding: 15px 20px;
    margin: 15px 0;
    border-radius: 4px;
}
.erros-fatais strong {
    color: #C62828;
}
"""

# ── Append watermark CSS dynamically (SVG data URI is too large for static string) ──
CSS += f"""
.content-page {{
    page: content;
    position: relative;
}}
.content-page::before {{
    content: "";
    position: fixed;
    top: -30%;
    left: -30%;
    width: 160%;
    height: 160%;
    background-image: url("{_WATERMARK_URI}");
    background-repeat: repeat;
    background-size: 80mm auto;
    transform: rotate(-30deg);
    transform-origin: center center;
    pointer-events: none;
    z-index: -1;
}}
"""


# ── HTML Content ─────────────────────────────────────────────────────────
HTML_CONTENT = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<style>{CSS}</style>
</head>
<body>

<!-- Social footer fixo em todas as páginas -->
<div class="social-footer">
    <a href="https://www.linkedin.com/company/boostresearch">
        <svg viewBox="0 0 24 24" fill="#0A66C2"><path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433a2.062 2.062 0 01-2.063-2.065 2.064 2.064 0 112.063 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"/></svg>
        /boostresearch
    </a>
    <a href="https://www.instagram.com/cryptoandrefranco/">
        <svg viewBox="0 0 24 24" fill="#E1306C"><path d="M12 2.163c3.204 0 3.584.012 4.85.07 3.252.148 4.771 1.691 4.919 4.919.058 1.265.069 1.645.069 4.849 0 3.205-.012 3.584-.069 4.849-.149 3.225-1.664 4.771-4.919 4.919-1.266.058-1.644.07-4.85.07-3.204 0-3.584-.012-4.849-.07-3.26-.149-4.771-1.699-4.919-4.92-.058-1.265-.07-1.644-.07-4.849 0-3.204.013-3.583.07-4.849.149-3.227 1.664-4.771 4.919-4.919 1.266-.057 1.645-.069 4.849-.069zM12 0C8.741 0 8.333.014 7.053.072 2.695.272.273 2.69.073 7.052.014 8.333 0 8.741 0 12c0 3.259.014 3.668.072 4.948.2 4.358 2.618 6.78 6.98 6.98C8.333 23.986 8.741 24 12 24c3.259 0 3.668-.014 4.948-.072 4.354-.2 6.782-2.618 6.979-6.98.059-1.28.073-1.689.073-4.948 0-3.259-.014-3.667-.072-4.947-.196-4.354-2.617-6.78-6.979-6.98C15.668.014 15.259 0 12 0zm0 5.838a6.162 6.162 0 100 12.324 6.162 6.162 0 000-12.324zM12 16a4 4 0 110-8 4 4 0 010 8zm6.406-11.845a1.44 1.44 0 100 2.881 1.44 1.44 0 000-2.881z"/></svg>
        @cryptoandrefranco
    </a>
    <a href="https://elsonflorentino-afk.github.io/projetos-boost/cta-wa-ebook4.html" style="color:#25D366;">
        <svg viewBox="0 0 24 24" fill="#25D366"><path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.89-5.335 11.893-11.893a11.821 11.821 0 00-3.48-8.413z"/></svg>
        Fale com a Boost
    </a>
    <a href="https://boostresearch.com.br/" style="color:#00B37E;">
        <svg viewBox="0 0 24 24" fill="#00B37E"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 17.93c-3.95-.49-7-3.85-7-7.93 0-.62.08-1.21.21-1.79L9 15v1c0 1.1.9 2 2 2v1.93zm6.9-2.54c-.26-.81-1-1.39-1.9-1.39h-1v-3c0-.55-.45-1-1-1H8v-2h2c.55 0 1-.45 1-1V7h2c1.1 0 2-.9 2-2v-.41c2.93 1.19 5 4.06 5 7.41 0 2.08-.8 3.97-2.1 5.39z"/></svg>
        boostresearch.com.br
    </a>
</div>

<!-- CAPA -->
<div class="cover">
    <svg class="logo-img" viewBox="0 0 921 249" fill="none" xmlns="http://www.w3.org/2000/svg">
    <path fill-rule="evenodd" clip-rule="evenodd" d="M0.258436 1.064C-0.103564 1.65 -0.0825625 30.562 0.304437 65.314C1.08544 135.51 0.780437 132.635 7.83244 136.302C14.0304 139.525 16.9234 138.383 44.4764 121.836C58.5214 113.401 70.2254 105.967 70.4874 105.315C70.7484 104.663 69.5314 101.13 67.7814 97.464C66.0314 93.798 64.9824 90.439 65.4494 90.001C65.9164 89.562 79.0524 87.605 94.6414 85.652C119.596 82.526 123.72 81.755 129.141 79.203C145.237 71.627 154.691 55.82 153.571 38.36C152.652 24.046 143.201 11.342 127.702 3.58801L120.529 0H60.7234C19.5164 0 0.711436 0.330995 0.258436 1.064ZM140.619 85.36C139.893 86.108 129.623 99.137 117.798 114.314C105.973 129.491 96.0734 141.925 95.7984 141.945C95.5234 141.966 93.5814 139.317 91.4814 136.058C89.3824 132.8 87.1144 129.677 86.4414 129.118C85.5574 128.385 73.6074 135.061 43.4524 153.135C20.4814 166.902 1.83044 178.596 2.00544 179.12C2.19844 179.7 23.6944 180.306 56.8104 180.665C117.355 181.321 117.618 181.3 130.798 174.737C140.242 170.034 151.445 158.663 156.155 149C162.56 135.857 163.192 119.608 157.885 104.508C155.66 98.176 153.095 94.185 147.434 88.25C142.968 83.566 142.539 83.384 140.619 85.36Z" fill="white"/>
    <path d="M267.746 248.5V219.36H278.652C282.343 219.36 285.243 220.248 287.352 222.025C289.489 223.773 290.558 226.215 290.558 229.351C290.558 231.405 290.072 233.181 289.101 234.679C288.157 236.15 286.797 237.288 285.021 238.093C283.245 238.87 281.122 239.259 278.652 239.259H269.452L270.826 237.843V248.5H267.746ZM287.644 248.5L280.151 237.926H283.481L291.016 248.5H287.644ZM270.826 238.093L269.452 236.636H278.569C281.483 236.636 283.689 235.998 285.188 234.721C286.714 233.444 287.477 231.654 287.477 229.351C287.477 227.02 286.714 225.216 285.188 223.939C283.689 222.663 281.483 222.025 278.569 222.025H269.452L270.826 220.568V238.093ZM323.222 248.5V219.36H343.203V222.025H326.302V245.836H343.828V248.5H323.222ZM325.969 235.012V232.39H341.371V235.012H325.969ZM384.496 248.75C382.331 248.75 380.25 248.403 378.252 247.709C376.281 246.988 374.755 246.072 373.673 244.962L374.88 242.589C375.907 243.588 377.28 244.434 379.001 245.128C380.749 245.794 382.581 246.127 384.496 246.127C386.328 246.127 387.812 245.905 388.95 245.461C390.116 244.989 390.962 244.365 391.489 243.588C392.045 242.811 392.322 241.951 392.322 241.007C392.322 239.869 391.989 238.953 391.323 238.259C390.685 237.566 389.838 237.025 388.784 236.636C387.729 236.22 386.563 235.859 385.287 235.554C384.01 235.248 382.734 234.929 381.457 234.596C380.18 234.235 379.001 233.764 377.919 233.181C376.864 232.598 376.004 231.835 375.338 230.891C374.699 229.92 374.38 228.657 374.38 227.103C374.38 225.66 374.755 224.342 375.504 223.148C376.281 221.927 377.461 220.956 379.043 220.234C380.625 219.485 382.65 219.111 385.12 219.111C386.758 219.111 388.381 219.346 389.991 219.818C391.6 220.262 392.988 220.887 394.154 221.691L393.113 224.148C391.864 223.315 390.532 222.704 389.117 222.316C387.729 221.927 386.383 221.733 385.079 221.733C383.33 221.733 381.887 221.969 380.749 222.441C379.612 222.913 378.765 223.551 378.21 224.356C377.683 225.133 377.419 226.021 377.419 227.02C377.419 228.158 377.738 229.074 378.377 229.767C379.043 230.461 379.903 231.002 380.958 231.391C382.04 231.779 383.219 232.126 384.496 232.432C385.773 232.737 387.035 233.07 388.284 233.431C389.561 233.791 390.726 234.263 391.781 234.846C392.863 235.401 393.724 236.15 394.362 237.094C395.028 238.037 395.361 239.272 395.361 240.799C395.361 242.214 394.972 243.532 394.195 244.753C393.418 245.947 392.225 246.918 390.615 247.667C389.033 248.389 386.994 248.75 384.496 248.75ZM427.466 248.5V219.36H447.447V222.025H430.546V245.836H448.072V248.5H427.466ZM430.213 235.012V232.39H445.616V235.012H430.213ZM476.044 248.5L489.365 219.36H492.404L505.725 248.5H502.478L490.239 221.15H491.488L479.249 248.5H476.044ZM481.289 240.716L482.205 238.218H499.147L500.063 240.716H481.289ZM535.938 248.5V219.36H546.845C550.536 219.36 553.436 220.248 555.545 222.025C557.682 223.773 558.75 226.215 558.75 229.351C558.75 231.405 558.265 233.181 557.293 234.679C556.35 236.15 554.99 237.288 553.214 238.093C551.438 238.87 549.315 239.259 546.845 239.259H537.645L539.019 237.843V248.5H535.938ZM555.836 248.5L548.343 237.926H551.674L559.208 248.5H555.836ZM539.019 238.093L537.645 236.636H546.761C549.675 236.636 551.882 235.998 553.38 234.721C554.907 233.444 555.67 231.654 555.67 229.351C555.67 227.02 554.907 225.216 553.38 223.939C551.882 222.663 549.675 222.025 546.761 222.025H537.645L539.019 220.568V238.093ZM604.069 248.75C601.877 248.75 599.851 248.389 597.991 247.667C596.132 246.918 594.522 245.877 593.163 244.545C591.803 243.213 590.734 241.645 589.957 239.841C589.208 238.037 588.833 236.067 588.833 233.93C588.833 231.793 589.208 229.823 589.957 228.019C590.734 226.215 591.803 224.647 593.163 223.315C594.55 221.983 596.174 220.956 598.033 220.234C599.892 219.485 601.918 219.111 604.111 219.111C606.22 219.111 608.204 219.471 610.064 220.193C611.923 220.887 613.491 221.941 614.768 223.357L612.811 225.313C611.618 224.092 610.299 223.218 608.856 222.691C607.413 222.136 605.859 221.858 604.194 221.858C602.446 221.858 600.822 222.163 599.323 222.774C597.825 223.357 596.521 224.203 595.41 225.313C594.3 226.395 593.426 227.672 592.788 229.143C592.177 230.586 591.872 232.182 591.872 233.93C591.872 235.679 592.177 237.288 592.788 238.759C593.426 240.202 594.3 241.479 595.41 242.589C596.521 243.671 597.825 244.518 599.323 245.128C600.822 245.711 602.446 246.002 604.194 246.002C605.859 246.002 607.413 245.725 608.856 245.17C610.299 244.615 611.618 243.727 612.811 242.506L614.768 244.462C613.491 245.877 611.923 246.946 610.064 247.667C608.204 248.389 606.206 248.75 604.069 248.75ZM667.635 248.5V219.36H670.673V248.5H667.635ZM646.321 248.5V219.36H649.401V248.5H646.321ZM649.068 235.054V232.348H667.926V235.054H649.068Z" fill="white"/>
    <path d="M810.705 179V50.2034H759.635V4.61395H920.568V50.2034H869.498V179H810.705Z" fill="white"/>
    <path d="M676.562 182.986C661.947 182.986 647.83 181.325 634.211 178.003C620.593 174.682 609.382 170.364 600.58 165.049L619.513 122.2C627.817 127.016 637.035 130.919 647.166 133.909C657.463 136.732 667.428 138.144 677.06 138.144C682.707 138.144 687.108 137.812 690.264 137.147C693.586 136.317 695.994 135.237 697.489 133.909C698.983 132.414 699.731 130.67 699.731 128.677C699.731 125.522 697.987 123.03 694.499 121.203C691.011 119.377 686.361 117.882 680.548 116.719C674.901 115.391 668.673 114.062 661.864 112.733C655.055 111.239 648.162 109.329 641.187 107.003C634.377 104.678 628.066 101.606 622.253 97.7859C616.607 93.966 612.039 88.9836 608.552 82.8385C605.064 76.5274 603.32 68.7216 603.32 59.421C603.32 48.6257 606.31 38.8268 612.289 30.0245C618.434 21.0561 627.485 13.9145 639.443 8.59992C651.567 3.2853 666.597 0.627991 684.534 0.627991C696.326 0.627991 707.952 1.8736 719.411 4.36483C730.871 6.85606 741.168 10.6759 750.303 15.8245L732.615 58.4245C723.979 54.1064 715.591 50.8678 707.453 48.7087C699.482 46.5496 691.676 45.4701 684.036 45.4701C678.389 45.4701 673.905 45.9684 670.583 46.9648C667.262 47.9613 664.853 49.29 663.359 50.9508C662.03 52.6116 661.366 54.4385 661.366 56.4315C661.366 59.421 663.11 61.8292 666.597 63.6561C670.085 65.3169 674.652 66.7286 680.299 67.8912C686.112 69.0537 692.423 70.2994 699.232 71.628C706.208 72.9567 713.1 74.7836 719.91 77.1087C726.719 79.4339 732.947 82.5064 738.594 86.3263C744.407 90.1461 749.057 95.1286 752.545 101.274C756.032 107.419 757.776 115.058 757.776 124.193C757.776 134.822 754.704 144.621 748.559 153.589C742.58 162.392 733.611 169.533 721.653 175.014C709.696 180.329 694.665 182.986 676.562 182.986Z" fill="white"/>
    <path d="M491.799 182.986C477.682 182.986 464.561 180.744 452.437 176.26C440.479 171.775 430.099 165.464 421.297 157.326C412.495 149.022 405.602 139.306 400.62 128.179C395.803 117.051 393.395 104.927 393.395 91.807C393.395 78.5204 395.803 66.3964 400.62 55.435C405.602 44.3075 412.495 34.6748 421.297 26.5368C430.099 18.2327 440.479 11.8385 452.437 7.35431C464.561 2.8701 477.682 0.627991 491.799 0.627991C506.082 0.627991 519.202 2.8701 531.16 7.35431C543.118 11.8385 553.498 18.2327 562.3 26.5368C571.103 34.6748 577.912 44.3075 582.729 55.435C587.711 66.3964 590.202 78.5204 590.202 91.807C590.202 104.927 587.711 117.051 582.729 128.179C577.912 139.306 571.103 149.022 562.3 157.326C553.498 165.464 543.118 171.775 531.16 176.26C519.202 180.744 506.082 182.986 491.799 182.986ZM491.799 135.403C497.279 135.403 502.345 134.407 506.995 132.414C511.812 130.421 515.964 127.598 519.451 123.944C523.105 120.124 525.929 115.557 527.922 110.242C529.914 104.761 530.911 98.6163 530.911 91.807C530.911 84.9976 529.914 78.9356 527.922 73.621C525.929 68.1403 523.105 63.573 519.451 59.9192C515.964 56.0994 511.812 53.1929 506.995 51.1999C502.345 49.207 497.279 48.2105 491.799 48.2105C486.318 48.2105 481.169 49.207 476.353 51.1999C471.703 53.1929 467.551 56.0994 463.897 59.9192C460.409 63.573 457.669 68.1403 455.676 73.621C453.683 78.9356 452.686 84.9976 452.686 91.807C452.686 98.6163 453.683 104.761 455.676 110.242C457.669 115.557 460.409 120.124 463.897 123.944C467.551 127.598 471.703 130.421 476.353 132.414C481.169 134.407 486.318 135.403 491.799 135.403Z" fill="white"/>
    <path d="M280.628 182.986C266.511 182.986 253.391 180.744 241.267 176.26C229.309 171.775 218.929 165.464 210.126 157.326C201.324 149.022 194.432 139.306 189.449 128.179C184.633 117.051 182.225 104.927 182.225 91.807C182.225 78.5204 184.633 66.3964 189.449 55.435C194.432 44.3075 201.324 34.6748 210.126 26.5368C218.929 18.2327 229.309 11.8385 241.267 7.35431C253.391 2.8701 266.511 0.627991 280.628 0.627991C294.911 0.627991 308.032 2.8701 319.99 7.35431C331.947 11.8385 342.328 18.2327 351.13 26.5368C359.932 34.6748 366.742 44.3075 371.558 55.435C376.54 66.3964 379.032 78.5204 379.032 91.807C379.032 104.927 376.54 117.051 371.558 128.179C366.742 139.306 359.932 149.022 351.13 157.326C342.328 165.464 331.947 171.775 319.99 176.26C308.032 180.744 294.911 182.986 280.628 182.986ZM280.628 135.403C286.109 135.403 291.174 134.407 295.825 132.414C300.641 130.421 304.793 127.598 308.281 123.944C311.935 120.124 314.758 115.557 316.751 110.242C318.744 104.761 319.74 98.6163 319.74 91.807C319.74 84.9976 318.744 78.9356 316.751 73.621C314.758 68.1403 311.935 63.573 308.281 59.9192C304.793 56.0994 300.641 53.1929 295.825 51.1999C291.174 49.207 286.109 48.2105 280.628 48.2105C275.147 48.2105 269.999 49.207 265.183 51.1999C260.532 53.1929 256.38 56.0994 252.726 59.9192C249.239 63.573 246.498 68.1403 244.505 73.621C242.512 78.9356 241.516 84.9976 241.516 91.807C241.516 98.6163 242.512 104.761 244.505 110.242C246.498 115.557 249.239 120.124 252.726 123.944C256.38 127.598 260.532 130.421 265.183 132.414C269.999 134.407 275.147 135.403 280.628 135.403Z" fill="white"/>
    </svg>
    <div class="divider"></div>
    <h1>STABLECOINS</h1>
    <div class="subtitle">O Mecanismo de Dolarização de Portfólio<br>que Você Precisa Conhecer</div>
    <div class="author">André Franco</div>
    <div class="company">Boost Research</div>
    <div class="year">2026</div>
    <div class="disclaimer-cover">Este material tem caráter educacional e não constitui recomendação de investimento. As informações aqui apresentadas não devem ser interpretadas como oferta ou solicitação de compra ou venda de ativos.</div>
</div>

<!-- SUMÁRIO -->
<div class="toc content-page">
    <h2>Sumário</h2>
    <div class="toc-entry"><span class="toc-num">1</span><span class="toc-title">O que são stablecoins e por que importam</span></div>
    <div class="toc-entry"><span class="toc-num">2</span><span class="toc-title">USDT vs USDC vs outras: riscos de cada modelo</span></div>
    <div class="toc-entry"><span class="toc-num">3</span><span class="toc-title">Dolarização via stablecoins na prática</span></div>
    <div class="toc-entry"><span class="toc-num">4</span><span class="toc-title">Rendimento: lending institucional vs DeFi</span></div>
    <div class="toc-entry"><span class="toc-num">5</span><span class="toc-title">Tributação e regulação</span></div>
    <div class="toc-entry"><span class="toc-num">6</span><span class="toc-title">Custódia e segurança</span></div>
    <div class="toc-entry"><span class="toc-num">7</span><span class="toc-title">Stablecoins no contexto do seu portfólio</span></div>
    <div class="toc-entry"><span class="toc-num">8</span><span class="toc-title">Ter dólares digitais vs saber o que fazer</span></div>
</div>

<!-- CONTEÚDO E-BOOK 4 -->
<div class="content-page">
<h1>Stablecoins</h1>
</div>
<div class="content-page" style="page-break-before:always">
<h2>O Mecanismo de Dolarização de Portfólio que Você Precisa Conhecer</h2>
<p><strong>André Franco</strong>, Fundador da Boost Research</p>
<p>Boost Research, Consultoria de Investimentos em Cripto</p>
<hr />
<h2>Sumário</h2>
<ol>
<li><a href="#capítulo-1--o-que-são-stablecoins-e-por-que-importam-agora">O que são stablecoins e por que importam agora</a></li>
<li><a href="#capítulo-2--usdt-vs-usdc-vs-outras-riscos-de-cada-modelo">USDT vs USDC vs outras: riscos de cada modelo</a></li>
<li><a href="#capítulo-3--dolarização-via-stablecoins-como-funciona-na-prática">Dolarização via stablecoins: como funciona na prática</a></li>
<li><a href="#capítulo-4--rendimento-em-stablecoins-lending-institucional-vs-defi">Rendimento em stablecoins: lending institucional vs DeFi</a></li>
<li><a href="#capítulo-5--tributação-e-regulação-decripto-in-1888-bcb-e-mica">Tributação e regulação: DeCripto, IN 1888, BCB e MiCA</a></li>
<li><a href="#capítulo-6--custódia-e-segurança-onde-guardar-como-proteger">Custódia e segurança: onde guardar, como proteger</a></li>
<li><a href="#capítulo-7--stablecoins-no-contexto-do-seu-portfólio">Stablecoins no contexto do seu portfólio</a></li>
<li><a href="#capítulo-8--a-diferença-entre-ter-dólares-digitais-e-saber-o-que-fazer-com-eles">A diferença entre ter dólares digitais e saber o que fazer com eles</a></li>
</ol>
<hr />
</div>
<div class="content-page" style="page-break-before:always">
<h2>Capítulo 1 — O que são stablecoins e por que importam agora</h2>
<p>Existe uma categoria de criptoativos que não aparece nas manchetes sobre volatilidade. Não dobra de valor em uma semana. Não desaba 40% em um dia. E, ainda assim, movimenta mais de 200 bilhões de dólares em capitalização de mercado e processa trilhões em volume transacionado por ano. São as stablecoins.</p>
<p>Uma stablecoin é um criptoativo projetado para manter paridade com uma moeda fiduciária, geralmente o dólar americano. Cada unidade de uma stablecoin como USDC ou USDT busca valer exatamente 1 dólar, mantida por mecanismos de lastro ou colateralização. Na prática, é uma representação digital do dólar que opera em redes blockchain, 24 horas por dia, 7 dias por semana, sem depender de bancos ou horários comerciais.</p>
<h3>Por que o investidor brasileiro deveria prestar atenção</h3>
<p>O real é uma moeda que, historicamente, perde valor em relação ao dólar. Quem converteu R$1,00 em dólares há quinze anos e manteve a posição viu o poder de compra internacional crescer de forma significativa, simplesmente pela desvalorização da moeda brasileira. Esse não é um fenômeno pontual. É uma tendência estrutural em economias emergentes com inflação historicamente acima da dos países desenvolvidos.</p>
<p>Para o investidor com patrimônio relevante, manter 100% dos ativos denominados em reais equivale a fazer uma aposta concentrada na estabilidade da moeda brasileira. É uma decisão de risco, mesmo que não pareça. Stablecoins oferecem uma via de acesso ao dólar digital que não exige conta no exterior, remessa internacional ou burocracia cambial.</p>
<h3>O crescimento do mercado de stablecoins</h3>
<p>O mercado de stablecoins ultrapassou a marca de 200 bilhões de dólares em capitalização total, com crescimento superior a 50% em período recente. Esse crescimento não foi impulsionado por especulação. Foi impulsionado por uso. Stablecoins se tornaram a infraestrutura de liquidação do mercado de criptoativos, a base para protocolos de empréstimo descentralizado e, cada vez mais, um mecanismo de transferência internacional de valor.</p>
<p>Grandes instituições financeiras passaram a emitir ou integrar stablecoins em suas operações. O que era um nicho técnico se transformou em uma camada de infraestrutura financeira com adoção institucional crescente.</p>
<h3>Tipos de stablecoins: uma visão geral</h3>
<p>Nem todas as stablecoins funcionam da mesma forma. Existem três categorias principais.</p>
<p><strong>Stablecoins colateralizadas por moeda fiduciária.</strong> O modelo mais direto. Para cada stablecoin emitida, existe (ou deveria existir) um dólar correspondente em reserva, mantido em contas bancárias ou títulos de curto prazo. USDT e USDC seguem esse modelo.</p>
<p><strong>Stablecoins colateralizadas por criptoativos.</strong> Nesse caso, o lastro é composto por outros criptoativos, geralmente com sobrecolateralização. Se a stablecoin vale US$1, o colateral depositado pode valer US$1,50 ou mais, criando uma margem de segurança contra a volatilidade. DAI, da MakerDAO, é o exemplo mais conhecido.</p>
<p><strong>Stablecoins algorítmicas.</strong> Utilizam mecanismos de oferta e demanda, frequentemente combinados com tokens auxiliares, para manter a paridade. É o modelo de maior risco, como o colapso da TerraUSD (UST) demonstrou de forma inequívoca.</p>
<h3>A ponte entre dois mundos</h3>
<p>Stablecoins ocupam uma posição singular no mercado financeiro. São criptoativos que se comportam como instrumentos de câmbio. Permitem que o investidor transite entre o universo de finanças tradicionais e o ecossistema cripto com fluidez, sem a fricção regulatória e operacional das transferências bancárias internacionais.</p>
<p>Para o investidor que já possui um portfólio diversificado em renda fixa, ações e fundos imobiliários, stablecoins representam um mecanismo de exposição cambial complementar. Não substitui a renda fixa brasileira. Não compete com o CDI. Atende a uma função diferente: proteção do poder de compra internacional e acesso a rendimentos denominados em dólar.</p>
<h3>O que este e-book oferece</h3>
<p>Ao longo dos próximos capítulos, vamos detalhar cada aspecto que o investidor precisa conhecer antes de alocar patrimônio em stablecoins. Desde a comparação entre os principais tokens até os riscos reais envolvidos. Desde a mecânica operacional de compra até a tributação aplicável. Desde os rendimentos disponíveis até as formas corretas de custódia.</p>
<p>O objetivo não é convencer ninguém a comprar stablecoins. É oferecer a base de conhecimento necessária para que o investidor tome uma decisão informada, com clareza sobre oportunidades e riscos.</p>
<h3>Checklist do Capítulo</h3>
<ul>
<li>[ ] Stablecoins buscam paridade com o dólar, mas não são dólares em conta bancária</li>
<li>[ ] O mercado já ultrapassou US$200 bilhões em capitalização</li>
<li>[ ] Existem três modelos principais de funcionamento, com riscos distintos</li>
<li>[ ] Manter 100% do patrimônio em reais é, por si só, uma decisão de risco cambial</li>
<li>[ ] Stablecoins são uma via de acesso ao dólar, não a única</li>
</ul>
<hr />
</div>
<div class="content-page" style="page-break-before:always">
<h2>Capítulo 2 — USDT vs USDC vs outras: riscos de cada modelo</h2>
<p>Dizer que uma stablecoin "vale um dólar" é como dizer que um carro "tem quatro rodas". É verdade, mas não diz nada sobre a engenharia por trás, a confiabilidade do motor ou a probabilidade de o veículo parar no meio da estrada. No mercado de stablecoins, a engenharia por trás da paridade faz toda a diferença.</p>
<h3>USDT (Tether): a maior e mais controversa</h3>
<p>A Tether (USDT) é a stablecoin mais negociada do mundo, com capitalização superior a 140 bilhões de dólares. Está presente em praticamente todas as exchanges, funciona como par de negociação padrão e possui a maior liquidez do mercado.</p>
<p>A controvérsia reside nas reservas. A Tether demorou anos para divulgar a composição de seu lastro e enfrentou questionamentos regulatórios significativos. As attestations publicadas pela empresa (diferentes de auditorias completas) mostram que as reservas incluem títulos do Tesouro americano, depósitos bancários e outros ativos. A transparência melhorou, mas permanece abaixo do padrão exigido por reguladores de mercados desenvolvidos.</p>
<p>Para o investidor, USDT oferece liquidez incomparável. Em contrapartida, carrega risco de governança corporativa e risco regulatório. Se um regulador importante restringir as operações da Tether, o impacto sobre a paridade pode ser imediato.</p>
<h3>USDC (Circle): transparência como diferencial</h3>
<p>A USDC, emitida pela Circle, posiciona-se como a alternativa regulada e transparente. Com capitalização na faixa de 60 bilhões de dólares, é a segunda maior stablecoin do mercado.</p>
<p>A Circle publica attestations mensais realizadas por uma das grandes firmas de auditoria. As reservas são compostas predominantemente por títulos do Tesouro americano de curto prazo e depósitos em instituições financeiras reguladas nos Estados Unidos. A empresa é registrada como transmissora de dinheiro (Money Services Business) e opera sob supervisão regulatória.</p>
<p>O episódio mais relevante de risco ocorreu quando o Silicon Valley Bank (SVB) colapsou. A Circle mantinha parte de suas reservas no SVB, e a USDC chegou a perder temporariamente a paridade, negociando a US$0,87 antes de se recuperar após o Federal Reserve garantir os depósitos.</p>
<p>Esse evento demonstrou um ponto fundamental: mesmo uma stablecoin bem gerida carrega riscos que vão além do seu controle direto.</p>
<h3>DAI (MakerDAO): descentralização com complexidade</h3>
<p>A DAI adota um modelo diferente. Em vez de reservas em conta bancária, é colateralizada por criptoativos depositados em contratos inteligentes no protocolo Maker. Para gerar DAI, o usuário deposita colateral (ETH, WBTC e outros ativos aceitos) com taxa de sobrecolateralização que normalmente exige 150% ou mais do valor da DAI gerada.</p>
<p>O modelo é transparente por natureza, já que todo o colateral está visível na blockchain. A governança é descentralizada, exercida por detentores do token MKR. A desvantagem é a complexidade. O investidor médio não precisa entender os mecanismos de liquidação de vaults para comprar DAI em uma exchange. Mas deveria saber que, em cenários de queda abrupta do mercado cripto, o sistema de liquidação pode ser estressado.</p>
<h3>A lição da Terra/Luna: US$40 bilhões que desapareceram</h3>
<p>Em meados de 2022, o ecossistema Terra colapsou. A TerraUSD (UST), uma stablecoin algorítmica que mantinha paridade por meio de um mecanismo de arbitragem com o token LUNA, perdeu completamente sua âncora. Em poucos dias, US$40 bilhões em valor de mercado foram eliminados.</p>
<p>O colapso da Terra não foi um acidente isolado. Foi a consequência previsível de um modelo que dependia de incentivos econômicos circulares sem lastro externo real. Quando a confiança no mecanismo diminuiu, a espiral de venda se tornou irreversível.</p>
<p>A lição é direta: stablecoins algorítmicas sem colateral externo adequado carregam risco de perda total. Não é um risco teórico. É um risco materializado, documentado e que destruiu patrimônio real de investidores em todo o mundo.</p>
<p>André Franco acompanhou o colapso de Terra/Luna em tempo real com os investidores da Boost Research. Essa experiência reforçou a importância de analisar o modelo de lastro antes de considerar qualquer stablecoin.</p>
<h3>Comparativo de risco</h3>
<table class="data-table">
<thead>
<tr>
<th>Critério</th>
<th>USDT (Tether)</th>
<th>USDC (Circle)</th>
<th>DAI (MakerDAO)</th>
</tr>
</thead>
<tbody>
<tr>
<td>Capitalização</td>
<td>~US$140 bi</td>
<td>~US$60 bi</td>
<td>~US$5 bi</td>
</tr>
<tr>
<td>Transparência de reservas</td>
<td>Parcial (attestations)</td>
<td>Alta (attestations mensais)</td>
<td>Total (on-chain)</td>
</tr>
<tr>
<td>Regulação</td>
<td>Limitada</td>
<td>Regulada nos EUA</td>
<td>Governança descentralizada</td>
</tr>
<tr>
<td>Histórico de depeg</td>
<td>Breves desvios, nunca colapsou</td>
<td>Depeg temporário (SVB)</td>
<td>Desvios pontuais em crises</td>
</tr>
<tr>
<td>Liquidez</td>
<td>Muito alta</td>
<td>Alta</td>
<td>Moderada</td>
</tr>
<tr>
<td>Risco principal</td>
<td>Governança e regulatório</td>
<td>Contraparte bancária</td>
<td>Volatilidade do colateral cripto</td>
</tr>
</tbody>
</table>
<h3>Outras stablecoins relevantes</h3>
<p><strong>BUSD.</strong> Emitida pela Binance em parceria com a Paxos, foi descontinuada após ação regulatória nos Estados Unidos. Serve como lembrete de que reguladores podem encerrar stablecoins específicas.</p>
<p><strong>TUSD (TrueUSD).</strong> Menor participação de mercado, com attestations realizadas por terceiros. Enfrentou questionamentos sobre a real composição das reservas.</p>
<p><strong>FRAX.</strong> Modelo híbrido, parcialmente colateralizado e parcialmente algorítmico. Migrou progressivamente para colateralização total após o colapso da Terra.</p>
<h3>A mensagem central</h3>
<p>Não existe stablecoin sem risco. O que existe é uma gradação de riscos diferentes. A escolha entre USDT, USDC, DAI ou qualquer outra stablecoin é uma decisão que deve considerar liquidez, transparência, jurisdição regulatória e tolerância ao risco de contraparte. Para o investidor com patrimônio relevante, entender essas diferenças não é opcional. É parte da diligência mínima antes de alocar capital.</p>
<h3>Checklist do Capítulo</h3>
<ul>
<li>[ ] USDT tem a maior liquidez, mas transparência de reservas ainda é parcial</li>
<li>[ ] USDC oferece maior clareza regulatória, porém o episódio SVB revelou riscos de contraparte bancária</li>
<li>[ ] DAI funciona com colateral em criptoativos verificável na blockchain, e os riscos de liquidação devem ser compreendidos</li>
<li>[ ] O colapso da Terra/Luna eliminou US$40 bilhões e demonstrou o risco real de stablecoins algorítmicas</li>
<li>[ ] Nenhuma stablecoin equivale a um depósito bancário garantido pelo FGC</li>
</ul>
<hr />
</div>
<div class="content-page" style="page-break-before:always">
<h2>Capítulo 3 — Dolarização via stablecoins: como funciona na prática</h2>
<p>A teoria já foi apresentada. Este capítulo é operacional. Detalha o passo a passo para que o investidor brasileiro converta reais em stablecoins, os custos envolvidos, os cenários práticos e a comparação com outras formas de dolarização.</p>
<h3>O caminho do real ao dólar digital</h3>
<p>O processo básico segue cinco etapas.</p>
<p><strong>1. Escolha da exchange.</strong> O investidor seleciona uma corretora de criptoativos (exchange) que aceite depósitos em reais via Pix ou TED. As principais exchanges que operam no Brasil oferecem essa funcionalidade.</p>
<p><strong>2. Depósito em reais.</strong> O valor é transferido da conta bancária para a conta na exchange. Depósitos via Pix costumam ser instantâneos. Via TED, o prazo varia conforme a exchange.</p>
<p><strong>3. Compra da stablecoin.</strong> Na exchange, o investidor compra USDC, USDT ou outra stablecoin utilizando o saldo em reais. A cotação reflete o câmbio USD/BRL do momento, acrescido do spread da plataforma.</p>
<p><strong>4. Decisão de custódia.</strong> A stablecoin pode permanecer na exchange (custódia terceirizada) ou ser transferida para uma carteira pessoal (autocustódia). O Capítulo 6 detalha as opções de custódia.</p>
<p><strong>5. Uso ou manutenção.</strong> O investidor pode manter a posição como reserva de valor em dólar, alocar em protocolos de rendimento (Capítulo 4) ou utilizar para pagamentos e transferências internacionais.</p>
<h3>Custos envolvidos</h3>
<p>A dolarização via stablecoins não é gratuita. Os custos incluem:</p>
<table class="data-table">
<thead>
<tr>
<th>Componente</th>
<th>Faixa típica</th>
<th>Observação</th>
</tr>
</thead>
<tbody>
<tr>
<td>Spread de câmbio na exchange</td>
<td>0,5% a 2,0%</td>
<td>Varia conforme liquidez e plataforma</td>
</tr>
<tr>
<td>Taxa de negociação (trading fee)</td>
<td>0,1% a 0,5%</td>
<td>Geralmente maker/taker com desconto por volume</td>
</tr>
<tr>
<td>Taxa de saque (withdrawal fee)</td>
<td>US$1 a US$25</td>
<td>Depende da rede blockchain escolhida (ERC-20 é mais caro, TRC-20 e redes L2 são mais baratas)</td>
</tr>
<tr>
<td>Gas fee (taxa de rede)</td>
<td>Variável</td>
<td>Custo de transação na blockchain, flutuante conforme congestionamento</td>
</tr>
</tbody>
</table>
<p>Para um investidor que converte R$100 mil em USDC, o custo total estimado fica entre 0,8% e 2,5%, dependendo da exchange e da rede utilizada. Em reais, isso representa de R$800 a R$2.500.</p>
<h3>Comparação: stablecoin vs outras formas de dolarização</h3>
<table class="data-table">
<thead>
<tr>
<th>Método</th>
<th>Custo estimado</th>
<th>Prazo</th>
<th>Burocracia</th>
<th>Rendimento em USD</th>
</tr>
</thead>
<tbody>
<tr>
<td>Stablecoin via exchange</td>
<td>0,8% a 2,5%</td>
<td>Minutos a horas</td>
<td>Baixa (cadastro na exchange)</td>
<td>Sim (DeFi/CeFi, com riscos)</td>
</tr>
<tr>
<td>Casa de câmbio (dólar espécie)</td>
<td>2% a 5% (spread)</td>
<td>Imediato</td>
<td>Baixa</td>
<td>Não</td>
</tr>
<tr>
<td>Remessa internacional (conta no exterior)</td>
<td>1% a 3% (spread + IOF 1,1%)</td>
<td>1 a 3 dias úteis</td>
<td>Alta (abertura de conta, compliance)</td>
<td>Sim (depende da jurisdição)</td>
</tr>
<tr>
<td>ETF dolarizado (IVVB11, por exemplo)</td>
<td>Taxa de administração ~0,23% a.a. + corretagem</td>
<td>D+2 (bolsa)</td>
<td>Baixa (conta em corretora)</td>
<td>Indireta (valorização do ativo)</td>
</tr>
<tr>
<td>Fundo cambial</td>
<td>Taxa administração 0,5% a 1,5% a.a. + come-cotas</td>
<td>D+1 a D+3</td>
<td>Baixa</td>
<td>Não (apenas variação cambial)</td>
</tr>
</tbody>
</table>
<p>Cada método atende a um perfil diferente. Stablecoins se destacam pela agilidade e pela possibilidade de gerar rendimento em dólar. Perdem para ETFs e fundos cambiais na simplicidade tributária e na familiaridade do investidor tradicional.</p>
<h3>Cenários práticos</h3>
<div class="risk-box">
<p><strong>Simulações baseadas em dados históricos e projeções. Rentabilidade passada não garante resultados futuros.</strong></p>
</div>
<p><strong>Cenário A: R$100 mil em stablecoins, dólar se valoriza 10% em 12 meses.</strong></p>
<p>O investidor converte R$100 mil em aproximadamente US$17.500 (considerando câmbio de R$5,70 e custos de 1,5%). Se o dólar sobe 10% em relação ao real, a posição passa a valer cerca de R$107.250 quando convertida de volta, descontados custos de reconversão. Caso o investidor também tenha alocado em protocolo de rendimento a 5% a.a. em USD, o retorno bruto seria de aproximadamente R$112.600.</p>
<p><strong>Cenário B: R$500 mil em stablecoins, dólar permanece estável.</strong></p>
<p>A posição em dólar não gera ganho cambial. O rendimento vem exclusivamente do yield em protocolo. A 5% a.a. sobre US$87.500 (após custos), o retorno bruto seria de US$4.375, ou cerca de R$24.900, sem considerar variação cambial. Os custos de entrada e saída (totalizando cerca de 3% entre ida e volta) consomem parte relevante desse rendimento.</p>
<p><strong>Cenário C: R$1 milhão com 10% em stablecoins, dólar se desvaloriza 5%.</strong></p>
<p>A parcela de R$100 mil alocada em stablecoins sofre perda cambial. Mesmo com rendimento de 5% em USD, a desvalorização do dólar resulta em retorno próximo de zero ou levemente negativo quando convertido para reais. Os outros 90% do portfólio, alocados em renda fixa e variável brasileira, compensam essa perda. É por isso que stablecoins funcionam como parcela do portfólio, não como alocação total.</p>
<h3>Desvalorização histórica do real</h3>
<p>O histórico cambial brasileiro reforça a importância da diversificação em moeda forte. A trajetória do USD/BRL ao longo das últimas décadas mostra uma tendência estrutural de depreciação do real. Períodos de valorização existem, mas a tendência de longo prazo é consistente.</p>
<p>Isso não significa que o dólar sempre sobe. Significa que, para o investidor que pensa em décadas e não em meses, a exposição cambial é um componente de proteção patrimonial, não de especulação.</p>
<h3>Quando faz sentido e quando não faz</h3>
<p><strong>Faz sentido quando:</strong> o investidor tem patrimônio relevante concentrado em reais, horizonte de longo prazo, familiaridade mínima com o ecossistema cripto e disposição para entender os riscos operacionais e técnicos envolvidos.</p>
<p><strong>Não faz sentido quando:</strong> o investidor não possui reserva de emergência em reais, não entende os riscos de custódia e smart contract, busca retorno de curto prazo com base em expectativa de câmbio ou pretende alocar 100% do patrimônio em um único instrumento.</p>
<h3>Checklist do Capítulo</h3>
<ul>
<li>[ ] O processo de compra envolve cinco etapas, da escolha da exchange à decisão de custódia</li>
<li>[ ] Custos totais de conversão variam entre 0,8% e 2,5% por operação</li>
<li>[ ] Stablecoins devem ser comparadas com remessa internacional, ETF e fundo cambial antes de decidir</li>
<li>[ ] A desvalorização do dólar pode anular o rendimento em USD quando convertido para reais</li>
<li>[ ] Stablecoins funcionam como parcela do portfólio, nunca como alocação total</li>
</ul>
<hr />
</div>
<div class="content-page" style="page-break-before:always">
<h2>Capítulo 4 — Rendimento em stablecoins: lending institucional vs DeFi</h2>
<p>Manter stablecoins paradas em uma carteira é como ter dólares em espécie no cofre. Preserva o valor nominal, mas não gera retorno. Para o investidor que busca rendimento, existem protocolos e plataformas que remuneram a alocação de stablecoins. Esse rendimento, denominado em dólar, pode ser atrativo. Mas os riscos são proporcionais e, em alguns casos, incluem a perda total do capital.</p>
<h3>DeFi: protocolos de empréstimo descentralizado</h3>
<p>DeFi (Decentralized Finance) é o ecossistema de protocolos financeiros que operam em blockchain sem intermediários centralizados. Os dois protocolos de empréstimo mais consolidados são Aave e Compound.</p>
<p><strong>Como funciona.</strong> O investidor deposita stablecoins em um smart contract (contrato inteligente). Esses fundos são disponibilizados para tomadores de empréstimo que, por sua vez, depositam colateral cripto para garantir a operação. O rendimento do depositante vem dos juros pagos pelos tomadores.</p>
<p><strong>Taxas de rendimento.</strong> Os yields em stablecoins nesses protocolos variam tipicamente entre 3% e 8% ao ano em USD, dependendo da demanda por empréstimos e das condições de mercado. Em períodos de alta atividade no mercado cripto, os yields podem subir temporariamente acima desse patamar. Em períodos de baixa, podem cair para menos de 1%.</p>
<p><strong>Auditorias.</strong> Tanto Aave quanto Compound passaram por múltiplas auditorias de segurança realizadas por firmas especializadas. Isso reduz, mas não elimina, o risco de vulnerabilidades em smart contracts.</p>
<h3>CeFi: plataformas centralizadas de rendimento</h3>
<p>Plataformas centralizadas (CeFi) operam de forma similar a bancos digitais de criptoativos. O investidor deposita stablecoins e a plataforma gerencia o empréstimo ou investimento dos fundos, pagando rendimento ao depositante.</p>
<p>Os riscos de CeFi são diferentes dos de DeFi. Enquanto em DeFi o risco principal é técnico (falhas em smart contracts), em CeFi o risco principal é de contraparte. A plataforma pode tomar decisões de investimento ruins, enfrentar corrida de saques ou simplesmente fraudar os depositantes.</p>
<p>Os colapsos de Celsius, BlockFi, Voyager e, mais dramaticamente, FTX, demonstraram que o modelo "deposite aqui e ganhe rendimento" em plataformas centralizadas carrega risco existencial. Todas essas plataformas ofereciam yields atrativos e pareciam sólidas antes de colapsarem.</p>
<h3>Comparativo: DeFi vs CeFi</h3>
<table class="data-table">
<thead>
<tr>
<th>Critério</th>
<th>DeFi (Aave, Compound)</th>
<th>CeFi (plataformas centralizadas)</th>
</tr>
</thead>
<tbody>
<tr>
<td>Custódia</td>
<td>Smart contract (código auditado)</td>
<td>Empresa centralizada</td>
</tr>
<tr>
<td>Transparência</td>
<td>Total (on-chain)</td>
<td>Parcial a nenhuma</td>
</tr>
<tr>
<td>Yield típico em stablecoins</td>
<td>3% a 8% a.a.</td>
<td>Variável (historicamente 4% a 12%)</td>
</tr>
<tr>
<td>Risco principal</td>
<td>Smart contract, exploit, depeg</td>
<td>Contraparte, fraude, insolvência</td>
</tr>
<tr>
<td>Garantia de retorno</td>
<td>Nenhuma</td>
<td>Nenhuma</td>
</tr>
<tr>
<td>Histórico de perdas</td>
<td>Euler Finance, Curve exploits</td>
<td>Celsius, BlockFi, Voyager, FTX</td>
</tr>
</tbody>
</table>
<h3>Riscos que devem ser compreendidos antes de qualquer alocação</h3>
<p>Este é o trecho mais importante do capítulo. Rendimento em stablecoins não é renda fixa. Não é garantido. Não é protegido por nenhum mecanismo equivalente ao FGC. Os riscos a seguir são reais e já se materializaram.</p>
<p><strong>Risco de smart contract.</strong> Contratos inteligentes são código. Código pode ter vulnerabilidades. Hackers exploram essas vulnerabilidades para drenar fundos. Em período recente, o protocolo Euler Finance perdeu cerca de US$197 milhões em um único exploit. Parte dos fundos foi posteriormente recuperada, mas nem sempre isso acontece. O protocolo Curve sofreu exploit que comprometeu pools específicas. A lista de incidentes é extensa e contínua.</p>
<p><strong>Risco de depeg.</strong> A stablecoin depositada pode perder paridade com o dólar. Se o investidor tem USDC alocada em um protocolo DeFi e a USDC sofre depeg (como ocorreu brevemente durante a crise do SVB), o valor da posição cai proporcionalmente, mesmo que o protocolo de empréstimo funcione corretamente.</p>
<p><strong>Risco regulatório.</strong> Reguladores podem restringir ou proibir protocolos DeFi em jurisdições específicas. Mudanças regulatórias podem afetar a operação de exchanges que servem como ponte de acesso a esses protocolos.</p>
<p><strong>Risco de liquidez.</strong> Em momentos de estresse no mercado, a liquidez em protocolos DeFi pode secar. Retirar grandes volumes pode resultar em slippage (diferença entre o preço esperado e o preço executado) ou, em cenários extremos, na impossibilidade temporária de saque.</p>
<p><strong>Perda impermanente.</strong> Para investidores que fornecem liquidez em pools de pares (por exemplo, USDC/USDT), existe o risco de perda impermanente caso a relação de preço entre os ativos se desvie. Em pools de stablecoins, esse risco é menor, mas não é zero.</p>
<p>Na Boost Research, a avaliação de protocolos DeFi segue critérios rigorosos. André Franco e a equipe de Advisors analisam histórico de auditoria, volume de liquidez e tempo de operação antes de considerar qualquer protocolo como opção para investidores.</p>
<h3>Esses rendimentos não são garantidos</h3>
<p>Vale repetir de forma inequívoca: yields em stablecoins, seja em DeFi ou CeFi, podem cair a 0% e o capital investido pode ser perdido integralmente. Protocolos podem ser hackeados. Plataformas centralizadas podem colapsar. Stablecoins podem perder paridade. Esses não são cenários hipotéticos. São eventos que já ocorreram, mais de uma vez.</p>
<p>O investidor que considera alocar patrimônio em protocolos de rendimento precisa tratar essa decisão com o mesmo rigor que aplicaria a qualquer investimento de risco. Percentual pequeno do portfólio. Diversificação entre protocolos. Preferência por protocolos com histórico longo e múltiplas auditorias. E disposição para perder 100% do valor alocado.</p>
<h3>Cenários de rendimento vs perda</h3>
<div class="risk-box">
<p><strong>Simulações baseadas em dados históricos e projeções. Rentabilidade passada não garante resultados futuros.</strong></p>
</div>
<table class="data-table">
<thead>
<tr>
<th>Cenário</th>
<th>Capital alocado (USD)</th>
<th>Yield anual</th>
<th>Retorno bruto (USD)</th>
<th>Risco materializado</th>
</tr>
</thead>
<tbody>
<tr>
<td>Otimista</td>
<td>US$17.500</td>
<td>7%</td>
<td>US$1.225</td>
<td>Nenhum</td>
</tr>
<tr>
<td>Base</td>
<td>US$17.500</td>
<td>4%</td>
<td>US$700</td>
<td>Nenhum</td>
</tr>
<tr>
<td>Estresse</td>
<td>US$17.500</td>
<td>1%</td>
<td>US$175</td>
<td>Yield comprimido</td>
</tr>
<tr>
<td>Exploit parcial</td>
<td>US$17.500</td>
<td>N/A</td>
<td>-US$8.750</td>
<td>Perda de 50% por hack</td>
</tr>
<tr>
<td>Exploit total</td>
<td>US$17.500</td>
<td>N/A</td>
<td>-US$17.500</td>
<td>Perda total</td>
</tr>
</tbody>
</table>
<p>A tabela acima ilustra que o cenário de perda total existe e não deve ser descartado como probabilidade remota.</p>
<h3>Checklist do Capítulo</h3>
<ul>
<li>[ ] Rendimento em stablecoins pode variar de 0% a 8% a.a. em USD, sem garantia</li>
<li>[ ] Protocolos DeFi auditados (Aave, Compound) oferecem transparência, mas não eliminam risco de exploit</li>
<li>[ ] Plataformas CeFi colapsaram com recursos de investidores. Celsius, BlockFi e FTX são exemplos reais</li>
<li>[ ] O cenário de perda total do capital alocado é possível e já se materializou</li>
<li>[ ] Alocação em rendimento de stablecoins deve ser tratada como investimento de risco, não como renda fixa</li>
</ul>
<hr />
</div>
<div class="content-page" style="page-break-before:always">
<h2>Capítulo 5 — Tributação e regulação: DeCripto, IN 1888, BCB e MiCA</h2>
<p>O investidor que aloca patrimônio em stablecoins precisa entender o enquadramento tributário e regulatório desses ativos. O cenário é complexo, está em evolução e varia conforme a jurisdição. Este capítulo apresenta o panorama brasileiro e as referências internacionais mais relevantes.</p>
<div class="risk-box">
<p><strong>Este conteúdo tem caráter educacional e não substitui orientação tributária profissional.</strong></p>
</div>
<h3>Stablecoins são criptoativos perante a legislação brasileira</h3>
<p>Segundo a Instrução Normativa RFB 1888, stablecoins são classificadas como criptoativos. Não importa se a stablecoin busca paridade com o dólar: para fins tributários, é tratada como qualquer outro criptoativo.</p>
<p>Isso tem consequências práticas diretas.</p>
<h3>Declaração no Imposto de Renda</h3>
<p>Stablecoins devem ser declaradas na ficha "Bens e Direitos" do Imposto de Renda sempre que o custo de aquisição por tipo de ativo ultrapasse R$5.000. Cada stablecoin (USDT, USDC, DAI) é declarada separadamente. O valor declarado é o custo de aquisição em reais na data da compra, não o valor de mercado.</p>
<h3>Tributação sobre ganho de capital</h3>
<p>Quando o investidor vende (aliena) criptoativos e o total das alienações no mês supera R$35.000, incide imposto de 15% sobre o ganho de capital. Atenção: o limite de R$35.000 refere-se ao total de alienações (vendas), não ao lucro.</p>
<p>Na prática, vender stablecoins por reais configura alienação. Se o dólar se valorizou entre a compra e a venda, existe ganho de capital tributável. Por exemplo: o investidor comprou US$17.500 em USDC quando o dólar estava a R$5,70, pagando R$99.750. Meses depois, com o dólar a R$6,27 (+10%), vende a posição por R$109.725. O ganho de capital é de R$9.975, tributável a 15% se o total de alienações no mês excedeu R$35.000.</p>
<h3>DeCripto: reporte automático a partir da data prevista para implementação do DeCripto</h3>
<p>O sistema DeCripto, segundo o Banco Central, representa uma evolução significativa na supervisão de criptoativos no Brasil. A partir de sua implementação, exchanges que operam no país passam a reportar automaticamente transações de criptoativos à Receita Federal e ao Banco Central.</p>
<p>Para o investidor, isso significa que a Receita Federal terá acesso detalhado a todas as operações realizadas em exchanges nacionais. Operações não declaradas serão facilmente identificáveis por cruzamento de dados.</p>
<h3>Instrução Normativa RFB 1888</h3>
<p>A IN 1888 estabelece as obrigações de reporte para operações com criptoativos. As exchanges nacionais já reportam transações mensalmente. Para operações realizadas em exchanges internacionais, o investidor pessoa física é responsável por reportar operações que totalizem R$30.000 ou mais no mês.</p>
<h3>Resolução BCB 521</h3>
<p>A Resolução 521 do Banco Central regula aspectos relacionados a câmbio e capitais internacionais. A compra de stablecoins, por se tratar de aquisição de ativo denominado em moeda estrangeira, se insere no contexto de regulação cambial. O investidor deve estar atento às obrigações de declaração de Capitais Brasileiros no Exterior (CBE) caso mantenha ativos no exterior acima dos limites estabelecidos pelo Banco Central.</p>
<h3>MiCA: o modelo europeu de referência</h3>
<p>A regulação europeia Markets in Crypto-Assets (MiCA) criou um framework específico para stablecoins, classificando-as em duas categorias distintas.</p>
<p><strong>E-Money Tokens (EMTs).</strong> Stablecoins referenciadas a uma única moeda fiduciária. USDC e USDT se enquadram nessa categoria na Europa. Precisam de licença de instituição de dinheiro eletrônico para emissão.</p>
<p><strong>Asset-Referenced Tokens (ARTs).</strong> Stablecoins referenciadas a uma cesta de ativos ou a mais de uma moeda. Exigem licença específica e requisitos de reserva mais rigorosos.</p>
<p>A MiCA representa a tentativa regulatória mais abrangente do mundo para stablecoins. Embora não se aplique diretamente ao investidor brasileiro, sinaliza a direção que a regulação global está tomando: maior supervisão, requisitos de reserva obrigatórios e proteção ao investidor.</p>
<h3>CARF e OCDE: troca internacional de informações</h3>
<p>O Brasil participa do Common Reporting Standard (CRS) da OCDE e do Crypto-Asset Reporting Framework (CARF). Esses acordos estabelecem a troca automática de informações fiscais entre países, incluindo dados sobre operações com criptoativos.</p>
<p>Para o investidor que opera em exchanges internacionais ou mantém stablecoins em protocolos DeFi acessados a partir do Brasil, a premissa deve ser de que essas informações estarão disponíveis para a Receita Federal, se não hoje, no futuro próximo.</p>
<h3>Exemplo prático: compra, valorização e venda</h3>
<p>Um investidor compra R$100.000 em USDC quando o câmbio está a R$5,70 por dólar, obtendo aproximadamente US$17.543. Doze meses depois, o dólar está a R$6,27 (+10%). Ele vende a posição inteira.</p>
<table class="data-table">
<thead>
<tr>
<th>Etapa</th>
<th>Valor</th>
</tr>
</thead>
<tbody>
<tr>
<td>Compra</td>
<td>R$100.000 (= ~US$17.543)</td>
</tr>
<tr>
<td>Venda (dólar +10%)</td>
<td>~R$110.000</td>
</tr>
<tr>
<td>Ganho de capital</td>
<td>~R$10.000</td>
</tr>
<tr>
<td>IR devido (15%)</td>
<td>~R$1.500</td>
</tr>
<tr>
<td>Total de alienações no mês</td>
<td>R$110.000 (supera R$35.000)</td>
</tr>
</tbody>
</table>
<p>Se o investidor também obteve yield de 5% em USD ao longo do ano, o rendimento em stablecoins (~US$877 = ~R$5.500) também configura ganho tributável, a ser apurado separadamente.</p>
<h3>O cenário regulatório está em evolução</h3>
<p>O investidor prudente assume que a tendência regulatória é de maior supervisão, não de menor. Isso não é necessariamente negativo. Regulação clara beneficia o mercado ao reduzir incertezas e aumentar a confiança institucional. Porém, exige que o investidor mantenha registros detalhados de todas as operações e esteja preparado para cumprir obrigações de reporte que podem se tornar mais exigentes.</p>
<h3>Checklist do Capítulo</h3>
<ul>
<li>[ ] Stablecoins são criptoativos perante a legislação brasileira (IN 1888)</li>
<li>[ ] Declaração obrigatória em "Bens e Direitos" quando custo de aquisição supera R$5.000 por tipo</li>
<li>[ ] Alienações acima de R$35.000 no mês geram tributação de 15% sobre ganho de capital</li>
<li>[ ] O sistema DeCripto ampliará significativamente a visibilidade da Receita Federal sobre operações cripto</li>
<li>[ ] MiCA na Europa e CARF na OCDE sinalizam a direção: mais supervisão, mais transparência</li>
</ul>
<hr />
</div>
<div class="content-page" style="page-break-before:always">
<h2>Capítulo 6 — Custódia e segurança: onde guardar, como proteger</h2>
<p>A custódia de stablecoins é, possivelmente, o aspecto mais negligenciado pelo investidor iniciante em criptoativos. Enquanto o dinheiro no banco está protegido pelo FGC até R$250 mil e por toda a infraestrutura regulatória do sistema financeiro, criptoativos podem ser perdidos de forma irrecuperável por um erro de custódia.</p>
<h3>Custódia em exchange (custodial)</h3>
<p>A opção mais simples. O investidor compra stablecoins na exchange e as mantém lá. A exchange gerencia as chaves privadas, oferece interface amigável e, em muitos casos, permite operações de rendimento diretamente na plataforma.</p>
<p><strong>Vantagem:</strong> Conveniência. O investidor não precisa gerenciar chaves privadas, seed phrases ou interagir com a blockchain diretamente.</p>
<p><strong>Risco:</strong> Contraparte. Se a exchange for hackeada, sofrer insolvência ou for fechada por reguladores, os ativos dos clientes podem ser comprometidos. O caso FTX é o exemplo mais emblemático: bilhões de dólares em ativos de clientes foram perdidos quando a exchange colapsou.</p>
<p>Para patrimônio relevante, a custódia integral em uma única exchange concentra risco de forma inaceitável. Se a exchange falhar, o investidor perde tudo.</p>
<h3>Hot wallet (carteira quente)</h3>
<p>Hot wallets são aplicativos ou extensões de navegador que armazenam as chaves privadas do investidor em dispositivos conectados à internet. MetaMask, Trust Wallet e Rabby são exemplos comuns.</p>
<p><strong>Vantagem:</strong> O investidor detém as chaves privadas. "Not your keys, not your coins" (se não são suas chaves, não são suas moedas) é um princípio fundamental do ecossistema cripto.</p>
<p><strong>Risco:</strong> Vulnerabilidade digital. Por estarem em dispositivos conectados à internet, hot wallets são suscetíveis a malware, phishing, keyloggers e outros vetores de ataque. Um clique em um link malicioso pode resultar na drenagem completa da carteira.</p>
<h3>Hardware wallet (carteira fria)</h3>
<p>Hardware wallets são dispositivos físicos dedicados (semelhantes a pen drives) que armazenam chaves privadas offline. Ledger e Trezor são os fabricantes mais estabelecidos.</p>
<p><strong>Vantagem:</strong> As chaves privadas nunca saem do dispositivo. Mesmo que o computador esteja comprometido, o atacante não consegue acessar as chaves. Transações precisam ser confirmadas fisicamente no dispositivo.</p>
<p><strong>Risco:</strong> Perda ou dano ao dispositivo. Nesse caso, a recuperação depende da seed phrase (frase de recuperação). Se a seed phrase for perdida ou comprometida, os ativos são irrecuperáveis.</p>
<p>Para investidores com mais de R$100.000 em criptoativos, a hardware wallet é o padrão mínimo recomendado de segurança. O custo do dispositivo (entre US$60 e US$200) é insignificante comparado ao patrimônio protegido.</p>
<h3>Multi-sig wallets (carteiras de múltiplas assinaturas)</h3>
<p>Carteiras multi-sig exigem a aprovação de múltiplas chaves privadas para autorizar uma transação. Por exemplo, uma configuração 2 de 3 requer que pelo menos 2 de 3 chaves distintas aprovem a operação. Gnosis Safe (agora Safe) é o padrão de mercado.</p>
<p><strong>Vantagem:</strong> Elimina o ponto único de falha. Mesmo que uma chave seja comprometida, o atacante não consegue movimentar fundos sem as demais.</p>
<p><strong>Risco:</strong> Complexidade operacional. Configurar e gerenciar uma multi-sig exige conhecimento técnico acima da média.</p>
<h3>Comparativo de custódia</h3>
<table class="data-table">
<thead>
<tr>
<th>Método</th>
<th>Segurança</th>
<th>Conveniência</th>
<th>Patrimônio indicado</th>
<th>Risco principal</th>
</tr>
</thead>
<tbody>
<tr>
<td>Exchange (custodial)</td>
<td>Baixa a média</td>
<td>Alta</td>
<td>Até R$50k</td>
<td>Falência/hack da exchange</td>
</tr>
<tr>
<td>Hot wallet</td>
<td>Média</td>
<td>Alta</td>
<td>Até R$100k</td>
<td>Malware, phishing</td>
</tr>
<tr>
<td>Hardware wallet</td>
<td>Alta</td>
<td>Média</td>
<td>Acima de R$100k</td>
<td>Perda da seed phrase</td>
</tr>
<tr>
<td>Multi-sig</td>
<td>Muito alta</td>
<td>Baixa</td>
<td>Acima de R$500k</td>
<td>Complexidade operacional</td>
</tr>
</tbody>
</table>
<h3>Boas práticas de segurança</h3>
<p><strong>Seed phrase.</strong> A frase de recuperação (geralmente 12 ou 24 palavras) é a única forma de recuperar o acesso a uma carteira em caso de perda do dispositivo. Deve ser armazenada offline, em local seguro, preferencialmente em mais de uma cópia física. Nunca deve ser digitada em sites, enviada por e-mail ou armazenada em nuvem.</p>
<p><strong>Autenticação em dois fatores (2FA).</strong> Toda exchange e toda plataforma de rendimento deve ter 2FA ativado, preferencialmente via aplicativo (Google Authenticator, Authy), nunca via SMS.</p>
<p><strong>Endereço de saque verificado.</strong> Exchanges permitem cadastrar endereços de saque pré-aprovados. Ativar essa funcionalidade impede que um invasor envie fundos para um endereço desconhecido, mesmo que consiga acessar a conta.</p>
<p><strong>Atualizações de firmware.</strong> Hardware wallets recebem atualizações de segurança. Manter o firmware atualizado é essencial.</p>
<p><strong>Diversificação de custódia.</strong> Assim como não se coloca todo o patrimônio em um único banco, não se coloca todas as stablecoins em uma única carteira ou exchange. Distribuir entre diferentes métodos e plataformas reduz o impacto de qualquer falha individual.</p>
<h3>O papel da Boost Research na custódia</h3>
<p>A Boost Research é uma consultoria de investimentos em criptoativos. A consultoria orienta investidores sobre práticas de custódia e segurança, mas não mantém custódia dos ativos dos clientes. Cada investidor é responsável pela guarda de seus próprios criptoativos, com orientação do Advisor dedicado sobre as melhores práticas para cada nível de patrimônio.</p>
<h3>Checklist do Capítulo</h3>
<ul>
<li>[ ] Custódia em exchange é conveniente, mas concentra risco de contraparte</li>
<li>[ ] Hardware wallet é o padrão mínimo para patrimônio acima de R$100 mil em cripto</li>
<li>[ ] A seed phrase é a chave de tudo. Perder a seed phrase significa perder os ativos</li>
<li>[ ] Multi-sig elimina ponto único de falha, mas exige conhecimento técnico</li>
<li>[ ] Diversificação de custódia segue o mesmo princípio da diversificação de investimentos</li>
</ul>
<hr />
</div>
<div class="content-page" style="page-break-before:always">
<h2>Capítulo 7 — Stablecoins no contexto do seu portfólio</h2>
<p>Os capítulos anteriores trataram das stablecoins de forma isolada: o que são, como funcionam, quais os riscos, como tributar. Este capítulo integra tudo ao que realmente importa para o investidor: a composição do portfólio como um todo.</p>
<h3>Por que a exposição ao dólar importa</h3>
<p>O investidor brasileiro que mantém 100% do patrimônio em ativos denominados em reais carrega uma aposta implícita na estabilidade da moeda brasileira. Essa aposta pode parecer natural, afinal, as despesas do dia a dia são em reais. Mas para quem pensa em preservação patrimonial de longo prazo, a concentração em uma única moeda emergente é um risco que merece atenção.</p>
<p>O real se desvalorizou de forma significativa frente ao dólar ao longo das últimas décadas. Um portfólio que incluísse parcela dolarizada teria preservado poder de compra internacional de maneira relevante. Não é possível prever o câmbio futuro, mas é possível observar que a tendência estrutural de economias emergentes com inflação acima da dos países desenvolvidos aponta na mesma direção.</p>
<h3>Stablecoins como hedge cambial</h3>
<p>Dentro do universo de instrumentos de exposição cambial disponíveis para o investidor brasileiro, stablecoins ocupam um nicho específico. São mais acessíveis que contas no exterior, mais líquidas que dólar espécie e permitem geração de rendimento em USD. Por outro lado, carregam riscos técnicos e regulatórios que ETFs e fundos cambiais não possuem.</p>
<p>A posição em stablecoins funciona como hedge (proteção) cambial quando o real se desvaloriza. Não é uma operação de câmbio no sentido regulatório tradicional. É a aquisição de um criptoativo que busca paridade com o dólar. A exposição ao dólar é indireta, mediada pela mecânica da stablecoin.</p>
<h3>Alocação: qual percentual faz sentido</h3>
<p>Não existe uma resposta universal. O percentual adequado depende do patrimônio total, do perfil de risco, do horizonte de investimento e da familiaridade com criptoativos.</p>
<p>Como referência, investidores institucionais e consultores de patrimônio em mercados desenvolvidos frequentemente recomendam entre 5% e 15% do portfólio em ativos denominados em moeda forte para investidores de países emergentes. Esse percentual pode ser distribuído entre diferentes instrumentos: ETFs internacionais, fundos cambiais, contas no exterior e, para quem tem familiaridade e apetite de risco, stablecoins.</p>
<div class="risk-box">
<p><strong>Simulações baseadas em dados históricos e projeções. Rentabilidade passada não garante resultados futuros.</strong></p>
</div>
<table class="data-table">
<thead>
<tr>
<th>Perfil</th>
<th>Alocação sugerida em dólar (total)</th>
<th>Parcela via stablecoins</th>
<th>Observação</th>
</tr>
</thead>
<tbody>
<tr>
<td>Conservador (R$200k-500k)</td>
<td>5% a 8%</td>
<td>0% a 3%</td>
<td>Priorizar ETFs e fundos cambiais</td>
</tr>
<tr>
<td>Moderado (R$500k-1M)</td>
<td>8% a 12%</td>
<td>2% a 5%</td>
<td>Combinação de ETF + stablecoins</td>
</tr>
<tr>
<td>Arrojado (acima de R$1M)</td>
<td>10% a 15%</td>
<td>5% a 10%</td>
<td>Diversificação entre instrumentos</td>
</tr>
</tbody>
</table>
<p><em>Percentuais ilustrativos, não constituem recomendação individualizada.</em></p>
<h3>Comparativo: stablecoin vs ETF dolarizado vs dólar direto</h3>
<table class="data-table">
<thead>
<tr>
<th>Critério</th>
<th>Stablecoin (USDC/USDT)</th>
<th>ETF (ex: IVVB11)</th>
<th>Dólar espécie</th>
</tr>
</thead>
<tbody>
<tr>
<td>Rendimento em USD</td>
<td>Sim (DeFi/CeFi, com risco)</td>
<td>Indireta (valorização do ativo-base)</td>
<td>Não</td>
</tr>
<tr>
<td>Liquidez</td>
<td>24/7, minutos</td>
<td>D+2, horário de bolsa</td>
<td>Imediata, limitada a casas de câmbio</td>
</tr>
<tr>
<td>Custos</td>
<td>0,8% a 2,5% por operação</td>
<td>Taxa admin + corretagem</td>
<td>2% a 5% spread</td>
</tr>
<tr>
<td>Tributação</td>
<td>15% ganho capital (&gt;R$35k/mês)</td>
<td>15% ganho capital (qualquer valor)</td>
<td>15% ganho capital</td>
</tr>
<tr>
<td>Risco específico</td>
<td>Smart contract, depeg, custódia</td>
<td>Risco de mercado do ativo-base</td>
<td>Roubo, deterioração</td>
</tr>
<tr>
<td>Regulação</td>
<td>Em evolução</td>
<td>Consolidada (CVM)</td>
<td>Consolidada (BCB)</td>
</tr>
<tr>
<td>Complexidade operacional</td>
<td>Média a alta</td>
<td>Baixa</td>
<td>Baixa</td>
</tr>
</tbody>
</table>
<p>Para o investidor que já possui ETFs internacionais na carteira, stablecoins oferecem um complemento com características distintas: rendimento em USD e operação fora do horário de bolsa. Não substituem ETFs. Complementam.</p>
<h3>O modelo de alocação da Boost Research</h3>
<p>A Boost Research utiliza um modelo de alocação que integra criptoativos, incluindo stablecoins, ao portfólio existente do investidor. O modelo não propõe a substituição de classes de ativos tradicionais. Propõe a adição de uma camada de exposição que historicamente apresentou baixa correlação com renda fixa e renda variável brasileiras. Correlação histórica não garante comportamento futuro.</p>
<p>O Advisor dedicado analisa o portfólio completo do investidor, identifica a concentração de risco cambial e patrimonial e constrói uma proposta de alocação que integra criptoativos de forma proporcional ao perfil de risco. Stablecoins, nesse contexto, podem ocupar a função de reserva cambial ou de base para geração de rendimento em USD, sempre com limites de exposição definidos.</p>
<p>Esse modelo é utilizado pelos investidores que a Boost acompanha desde 2016, com ajustes contínuos conforme a evolução do mercado e do cenário regulatório.</p>
<h3>Ponte com o E-book 3: stablecoins como a "perna cambial" do portfólio diversificado</h3>
<p>No E-book 3 desta série ("Quanto Rende 1 Milhão"), apresentamos seis cenários de alocação para R$1 milhão. Os cenários mais sofisticados incluíam uma parcela de 5% a 10% em exposição cambial. Stablecoins são o mecanismo que viabiliza essa parcela de forma operacional e com potencial de rendimento.</p>
<p>O investidor que leu o E-book 3 e se perguntou "como, na prática, eu dolarizo 5% a 10% do meu portfólio?" encontra neste e-book a resposta detalhada. A mecânica de compra (Capítulo 3), os rendimentos possíveis e seus riscos (Capítulo 4), a tributação (Capítulo 5) e a custódia (Capítulo 6) completam o quadro.</p>
<h3>Checklist do Capítulo</h3>
<ul>
<li>[ ] Manter 100% do patrimônio em reais é uma aposta na estabilidade da moeda brasileira</li>
<li>[ ] Stablecoins complementam ETFs e fundos cambiais, não os substituem</li>
<li>[ ] A alocação em ativos dolarizados tipicamente varia entre 5% e 15% do portfólio total</li>
<li>[ ] O modelo Boost integra stablecoins ao portfólio existente, sem substituir classes tradicionais</li>
<li>[ ] Stablecoins são o mecanismo prático para a "perna cambial" discutida no E-book 3</li>
</ul>
<hr />
</div>
<div class="content-page" style="page-break-before:always">
<h2>Capítulo 8 — A diferença entre ter dólares digitais e saber o que fazer com eles</h2>
<p>Comprar stablecoins é a parte simples. Qualquer pessoa com um celular, um CPF e uma conta em exchange consegue converter reais em USDC ou USDT em poucos minutos. A barreira de entrada é técnica, mas não é alta.</p>
<p>A diferença que separa o investidor que usa stablecoins de forma estruturada daquele que simplesmente compra e espera está no método. Saber quanto alocar. Em qual stablecoin. Com qual estratégia de custódia. Em qual protocolo de rendimento, se algum. Com qual limite de exposição. E, principalmente, entender como essa peça se encaixa no mosaico maior do portfólio.</p>
<h3>Stablecoins são ferramentas. O que importa é o método.</h3>
<p>Uma stablecoin, por si só, não protege patrimônio. Não gera rendimento. Não diversifica o portfólio. É um instrumento. Assim como uma ação da Petrobras não é uma "estratégia de investimento", uma posição em USDC não é uma "estratégia de dolarização". O instrumento é neutro. O que define o resultado é a estratégia que orienta seu uso.</p>
<p>O investidor que compra USDT sem saber a diferença entre colateralização e lastro algorítmico está assumindo risco sem consciência. O que deposita stablecoins em um protocolo DeFi atraído por yields de dois dígitos sem entender risco de smart contract está comprando bilhete de loteria com desconto aparente. O que mantém toda a posição em stablecoins em uma única exchange sem diversificação de custódia está repetindo o erro dos clientes da FTX.</p>
<p>Informação, por si só, também não é suficiente. Este e-book oferece a base de conhecimento. A aplicação dessa base ao contexto específico de cada investidor exige análise individualizada.</p>
<h3>A experiência que sustenta o método</h3>
<p>André Franco fundou a Boost Research e acompanha o mercado de criptoativos desde 2016. Nesse período, o mercado passou por múltiplos ciclos de alta e baixa, pelo colapso de exchanges, pelo surgimento e queda de stablecoins algorítmicas, pela evolução regulatória de dezenas de jurisdições e pela transformação de um mercado de nicho em uma classe de ativos reconhecida institucionalmente.</p>
<p>O modelo de alocação da Boost Research reflete a experiência acumulada de acompanhar investidores desde 2016. Ao longo de múltiplos ciclos de mercado, cada ajuste incorporou aprendizados práticos. O resultado é uma abordagem que não promete retornos extraordinários, mas que oferece estrutura, disciplina e acompanhamento contínuo.</p>
<h3>Advisor dedicado: o papel que faz diferença</h3>
<p>O investidor com patrimônio acima de R$200 mil tem necessidades que vão além de um acompanhamento semanal ou de uma recomendação genérica. Precisa de alguém que conheça seu portfólio completo, entenda seus objetivos de longo prazo e consiga traduzir os movimentos do mercado cripto em ações concretas e calibradas ao seu perfil.</p>
<p>O modelo da Boost Research atribui um Advisor dedicado a cada investidor. Esse profissional acompanha as posições, comunica mudanças relevantes, rebalanceia a alocação quando necessário e está disponível para esclarecer dúvidas de forma personalizada. Não é um chatbot. Não é um algoritmo. É um profissional que conhece o contexto do investidor e ajusta a orientação de acordo.</p>
<h3>A pergunta que fica</h3>
<p>Stablecoins representam uma das inovações mais relevantes do mercado financeiro recente. Permitem dolarização acessível, rendimento em moeda forte e operação 24 horas. Mas carregam riscos que não podem ser ignorados: smart contract, depeg, regulatório e de custódia.</p>
<p>A diferença entre usar stablecoins como ferramenta de preservação patrimonial e usá-las como fonte de frustração está no método. Na análise. No acompanhamento. Na orientação de quem já navegou os ciclos e conhece os riscos por experiência.</p>
<p>Quanto do seu patrimônio deveria estar em dólar digital? Essa é a pergunta que o Advisor da Boost Research pode ajudar a responder.</p>
<h3>Converse com um Advisor da Boost Research</h3>
<p>A primeira conversa é sem compromisso. O objetivo é simples: mapear sua alocação atual, entender sua exposição cambial e identificar se stablecoins fazem sentido no contexto do seu portfólio.</p>
<p><a href="https://elsonflorentino-afk.github.io/projetos-boost/cta-wa-ebook4.html" class="cta-btn">Falar com um Advisor da Boost Research</a></p>
<hr />
<p><strong>Boost Research</strong>, Consultoria de Investimentos em Cripto</p>
<p>Fundada por André Franco, a Boost Research atua na interseção entre o mercado financeiro tradicional e o universo de criptoativos. Com uma equipe de Advisors dedicados e um modelo de alocação proprietário, a consultoria auxilia investidores com patrimônio relevante a integrar criptoativos ao portfólio de forma estruturada, com método e acompanhamento contínuo.</p>
<hr />
</div>
<div class="content-page" style="page-break-before:always">
<h2>Aviso Legal</h2>
<p>Este e-book tem caráter exclusivamente educacional e informativo. O conteúdo aqui apresentado não constitui recomendação de investimento, consultoria financeira, fiscal ou tributária individualizada.</p>
<p>Todas as simulações, projeções e cenários apresentados são hipotéticos, baseados em dados históricos e premissas de mercado vigentes na data de elaboração. <strong>Rentabilidade passada não garante resultados futuros.</strong> Os retornos reais podem diferir significativamente das estimativas apresentadas.</p>
<p><strong>Stablecoins não são equivalentes a depósitos bancários.</strong> Não contam com a proteção do Fundo Garantidor de Créditos (FGC) nem com qualquer mecanismo de garantia governamental. Os riscos incluem, mas não se limitam a: risco de smart contract (vulnerabilidades em código que podem resultar em perda total), risco de depeg (perda de paridade com o dólar), risco regulatório (mudanças legislativas que podem restringir operações), risco de contraparte (insolvência de exchanges ou emissores) e risco de custódia (perda de acesso a chaves privadas).</p>
<p><strong>Protocolos DeFi apresentam riscos adicionais.</strong> Rendimentos em protocolos descentralizados não são garantidos, podem ser reduzidos a zero a qualquer momento e o capital alocado pode ser perdido integralmente em caso de exploit, hack ou falha no protocolo.</p>
<p><strong>A dolarização via stablecoins não constitui recomendação cambial.</strong> O conteúdo sobre exposição ao dólar tem finalidade educacional e ilustrativa. Decisões de alocação cambial devem considerar o contexto individual de cada investidor.</p>
<p><strong>Este conteúdo tem caráter educacional e não substitui orientação tributária profissional.</strong> As informações sobre tributação refletem o entendimento vigente e podem sofrer alterações legislativas. Consulte um contador ou advogado tributarista para orientação específica à sua situação.</p>
<p>As opiniões expressas neste e-book são do autor e não representam recomendações personalizadas. Antes de tomar qualquer decisão de investimento, consulte um profissional qualificado que possa avaliar seu perfil de risco, objetivos financeiros e situação patrimonial individual.</p>
<p>A Boost Research é uma consultoria de investimentos especializada em criptoativos e não se responsabiliza por decisões tomadas com base exclusivamente nas informações contidas neste material.</p>
<p><strong>André Franco</strong>, Fundador da Boost Research</p>
<hr />
<p><em>Boost Research, Consultoria de Investimentos em Cripto</em></p>
</div>

<!-- AVISO LEGAL -->
<div class="aviso-legal content-page">
<h2>Aviso Legal</h2>
<p>Este material tem caráter exclusivamente educacional e informativo. Não constitui recomendação de investimento, oferta ou solicitação de compra ou venda de qualquer ativo financeiro, incluindo stablecoins ou criptoativos.</p>
<p>Stablecoins não são equivalentes a depósitos bancários. Não possuem garantia do Fundo Garantidor de Créditos (FGC) nem de qualquer mecanismo governamental de proteção ao investidor.</p>
<p>Protocolos DeFi possuem riscos adicionais de smart contract, incluindo a possibilidade de perda total dos fundos depositados. Rendimentos em protocolos DeFi não são garantidos e podem variar significativamente.</p>
<p>A menção a dolarização de portfólio tem caráter educacional e não constitui recomendação cambial.</p>
<p>Este conteúdo tem caráter educacional e não substitui orientação tributária profissional. A legislação tributária está sujeita a alterações.</p>
<p>Investimentos em criptoativos envolvem riscos significativos, incluindo a possibilidade de perda total do capital investido. Rentabilidade passada não garanta resultados futuros.</p>
<p>&copy; 2026 Boost Research. Todos os direitos reservados.</p>
</div>

<!-- CTA FINAL -->
<div class="cta-page">
    <svg class="logo-img" viewBox="0 0 921 249" fill="none" xmlns="http://www.w3.org/2000/svg" style="width:280px;margin-bottom:40px;">
    <path fill-rule="evenodd" clip-rule="evenodd" d="M0.258436 1.064C-0.103564 1.65 -0.0825625 30.562 0.304437 65.314C1.08544 135.51 0.780437 132.635 7.83244 136.302C14.0304 139.525 16.9234 138.383 44.4764 121.836C58.5214 113.401 70.2254 105.967 70.4874 105.315C70.7484 104.663 69.5314 101.13 67.7814 97.464C66.0314 93.798 64.9824 90.439 65.4494 90.001C65.9164 89.562 79.0524 87.605 94.6414 85.652C119.596 82.526 123.72 81.755 129.141 79.203C145.237 71.627 154.691 55.82 153.571 38.36C152.652 24.046 143.201 11.342 127.702 3.58801L120.529 0H60.7234C19.5164 0 0.711436 0.330995 0.258436 1.064ZM140.619 85.36C139.893 86.108 129.623 99.137 117.798 114.314C105.973 129.491 96.0734 141.925 95.7984 141.945C95.5234 141.966 93.5814 139.317 91.4814 136.058C89.3824 132.8 87.1144 129.677 86.4414 129.118C85.5574 128.385 73.6074 135.061 43.4524 153.135C20.4814 166.902 1.83044 178.596 2.00544 179.12C2.19844 179.7 23.6944 180.306 56.8104 180.665C117.355 181.321 117.618 181.3 130.798 174.737C140.242 170.034 151.445 158.663 156.155 149C162.56 135.857 163.192 119.608 157.885 104.508C155.66 98.176 153.095 94.185 147.434 88.25C142.968 83.566 142.539 83.384 140.619 85.36Z" fill="white"/>
    <path d="M267.746 248.5V219.36H278.652C282.343 219.36 285.243 220.248 287.352 222.025C289.489 223.773 290.558 226.215 290.558 229.351C290.558 231.405 290.072 233.181 289.101 234.679C288.157 236.15 286.797 237.288 285.021 238.093C283.245 238.87 281.122 239.259 278.652 239.259H269.452L270.826 237.843V248.5H267.746ZM287.644 248.5L280.151 237.926H283.481L291.016 248.5H287.644ZM270.826 238.093L269.452 236.636H278.569C281.483 236.636 283.689 235.998 285.188 234.721C286.714 233.444 287.477 231.654 287.477 229.351C287.477 227.02 286.714 225.216 285.188 223.939C283.689 222.663 281.483 222.025 278.569 222.025H269.452L270.826 220.568V238.093ZM323.222 248.5V219.36H343.203V222.025H326.302V245.836H343.828V248.5H323.222ZM325.969 235.012V232.39H341.371V235.012H325.969ZM384.496 248.75C382.331 248.75 380.25 248.403 378.252 247.709C376.281 246.988 374.755 246.072 373.673 244.962L374.88 242.589C375.907 243.588 377.28 244.434 379.001 245.128C380.749 245.794 382.581 246.127 384.496 246.127C386.328 246.127 387.812 245.905 388.95 245.461C390.116 244.989 390.962 244.365 391.489 243.588C392.045 242.811 392.322 241.951 392.322 241.007C392.322 239.869 391.989 238.953 391.323 238.259C390.685 237.566 389.838 237.025 388.784 236.636C387.729 236.22 386.563 235.859 385.287 235.554C384.01 235.248 382.734 234.929 381.457 234.596C380.18 234.235 379.001 233.764 377.919 233.181C376.864 232.598 376.004 231.835 375.338 230.891C374.699 229.92 374.38 228.657 374.38 227.103C374.38 225.66 374.755 224.342 375.504 223.148C376.281 221.927 377.461 220.956 379.043 220.234C380.625 219.485 382.65 219.111 385.12 219.111C386.758 219.111 388.381 219.346 389.991 219.818C391.6 220.262 392.988 220.887 394.154 221.691L393.113 224.148C391.864 223.315 390.532 222.704 389.117 222.316C387.729 221.927 386.383 221.733 385.079 221.733C383.33 221.733 381.887 221.969 380.749 222.441C379.612 222.913 378.765 223.551 378.21 224.356C377.683 225.133 377.419 226.021 377.419 227.02C377.419 228.158 377.738 229.074 378.377 229.767C379.043 230.461 379.903 231.002 380.958 231.391C382.04 231.779 383.219 232.126 384.496 232.432C385.773 232.737 387.035 233.07 388.284 233.431C389.561 233.791 390.726 234.263 391.781 234.846C392.863 235.401 393.724 236.15 394.362 237.094C395.028 238.037 395.361 239.272 395.361 240.799C395.361 242.214 394.972 243.532 394.195 244.753C393.418 245.947 392.225 246.918 390.615 247.667C389.033 248.389 386.994 248.75 384.496 248.75ZM427.466 248.5V219.36H447.447V222.025H430.546V245.836H448.072V248.5H427.466ZM430.213 235.012V232.39H445.616V235.012H430.213ZM476.044 248.5L489.365 219.36H492.404L505.725 248.5H502.478L490.239 221.15H491.488L479.249 248.5H476.044ZM481.289 240.716L482.205 238.218H499.147L500.063 240.716H481.289ZM535.938 248.5V219.36H546.845C550.536 219.36 553.436 220.248 555.545 222.025C557.682 223.773 558.75 226.215 558.75 229.351C558.75 231.405 558.265 233.181 557.293 234.679C556.35 236.15 554.99 237.288 553.214 238.093C551.438 238.87 549.315 239.259 546.845 239.259H537.645L539.019 237.843V248.5H535.938ZM555.836 248.5L548.343 237.926H551.674L559.208 248.5H555.836ZM539.019 238.093L537.645 236.636H546.761C549.675 236.636 551.882 235.998 553.38 234.721C554.907 233.444 555.67 231.654 555.67 229.351C555.67 227.02 554.907 225.216 553.38 223.939C551.882 222.663 549.675 222.025 546.761 222.025H537.645L539.019 220.568V238.093ZM604.069 248.75C601.877 248.75 599.851 248.389 597.991 247.667C596.132 246.918 594.522 245.877 593.163 244.545C591.803 243.213 590.734 241.645 589.957 239.841C589.208 238.037 588.833 236.067 588.833 233.93C588.833 231.793 589.208 229.823 589.957 228.019C590.734 226.215 591.803 224.647 593.163 223.315C594.55 221.983 596.174 220.956 598.033 220.234C599.892 219.485 601.918 219.111 604.111 219.111C606.22 219.111 608.204 219.471 610.064 220.193C611.923 220.887 613.491 221.941 614.768 223.357L612.811 225.313C611.618 224.092 610.299 223.218 608.856 222.691C607.413 222.136 605.859 221.858 604.194 221.858C602.446 221.858 600.822 222.163 599.323 222.774C597.825 223.357 596.521 224.203 595.41 225.313C594.3 226.395 593.426 227.672 592.788 229.143C592.177 230.586 591.872 232.182 591.872 233.93C591.872 235.679 592.177 237.288 592.788 238.759C593.426 240.202 594.3 241.479 595.41 242.589C596.521 243.671 597.825 244.518 599.323 245.128C600.822 245.711 602.446 246.002 604.194 246.002C605.859 246.002 607.413 245.725 608.856 245.17C610.299 244.615 611.618 243.727 612.811 242.506L614.768 244.462C613.491 245.877 611.923 246.946 610.064 247.667C608.204 248.389 606.206 248.75 604.069 248.75ZM667.635 248.5V219.36H670.673V248.5H667.635ZM646.321 248.5V219.36H649.401V248.5H646.321ZM649.068 235.054V232.348H667.926V235.054H649.068Z" fill="white"/>
    <path d="M810.705 179V50.2034H759.635V4.61395H920.568V50.2034H869.498V179H810.705Z" fill="white"/>
    <path d="M676.562 182.986C661.947 182.986 647.83 181.325 634.211 178.003C620.593 174.682 609.382 170.364 600.58 165.049L619.513 122.2C627.817 127.016 637.035 130.919 647.166 133.909C657.463 136.732 667.428 138.144 677.06 138.144C682.707 138.144 687.108 137.812 690.264 137.147C693.586 136.317 695.994 135.237 697.489 133.909C698.983 132.414 699.731 130.67 699.731 128.677C699.731 125.522 697.987 123.03 694.499 121.203C691.011 119.377 686.361 117.882 680.548 116.719C674.901 115.391 668.673 114.062 661.864 112.733C655.055 111.239 648.162 109.329 641.187 107.003C634.377 104.678 628.066 101.606 622.253 97.7859C616.607 93.966 612.039 88.9836 608.552 82.8385C605.064 76.5274 603.32 68.7216 603.32 59.421C603.32 48.6257 606.31 38.8268 612.289 30.0245C618.434 21.0561 627.485 13.9145 639.443 8.59992C651.567 3.2853 666.597 0.627991 684.534 0.627991C696.326 0.627991 707.952 1.8736 719.411 4.36483C730.871 6.85606 741.168 10.6759 750.303 15.8245L732.615 58.4245C723.979 54.1064 715.591 50.8678 707.453 48.7087C699.482 46.5496 691.676 45.4701 684.036 45.4701C678.389 45.4701 673.905 45.9684 670.583 46.9648C667.262 47.9613 664.853 49.29 663.359 50.9508C662.03 52.6116 661.366 54.4385 661.366 56.4315C661.366 59.421 663.11 61.8292 666.597 63.6561C670.085 65.3169 674.652 66.7286 680.299 67.8912C686.112 69.0537 692.423 70.2994 699.232 71.628C706.208 72.9567 713.1 74.7836 719.91 77.1087C726.719 79.4339 732.947 82.5064 738.594 86.3263C744.407 90.1461 749.057 95.1286 752.545 101.274C756.032 107.419 757.776 115.058 757.776 124.193C757.776 134.822 754.704 144.621 748.559 153.589C742.58 162.392 733.611 169.533 721.653 175.014C709.696 180.329 694.665 182.986 676.562 182.986Z" fill="white"/>
    <path d="M491.799 182.986C477.682 182.986 464.561 180.744 452.437 176.26C440.479 171.775 430.099 165.464 421.297 157.326C412.495 149.022 405.602 139.306 400.62 128.179C395.803 117.051 393.395 104.927 393.395 91.807C393.395 78.5204 395.803 66.3964 400.62 55.435C405.602 44.3075 412.495 34.6748 421.297 26.5368C430.099 18.2327 440.479 11.8385 452.437 7.35431C464.561 2.8701 477.682 0.627991 491.799 0.627991C506.082 0.627991 519.202 2.8701 531.16 7.35431C543.118 11.8385 553.498 18.2327 562.3 26.5368C571.103 34.6748 577.912 44.3075 582.729 55.435C587.711 66.3964 590.202 78.5204 590.202 91.807C590.202 104.927 587.711 117.051 582.729 128.179C577.912 139.306 571.103 149.022 562.3 157.326C553.498 165.464 543.118 171.775 531.16 176.26C519.202 180.744 506.082 182.986 491.799 182.986ZM491.799 135.403C497.279 135.403 502.345 134.407 506.995 132.414C511.812 130.421 515.964 127.598 519.451 123.944C523.105 120.124 525.929 115.557 527.922 110.242C529.914 104.761 530.911 98.6163 530.911 91.807C530.911 84.9976 529.914 78.9356 527.922 73.621C525.929 68.1403 523.105 63.573 519.451 59.9192C515.964 56.0994 511.812 53.1929 506.995 51.1999C502.345 49.207 497.279 48.2105 491.799 48.2105C486.318 48.2105 481.169 49.207 476.353 51.1999C471.703 53.1929 467.551 56.0994 463.897 59.9192C460.409 63.573 457.669 68.1403 455.676 73.621C453.683 78.9356 452.686 84.9976 452.686 91.807C452.686 98.6163 453.683 104.761 455.676 110.242C457.669 115.557 460.409 120.124 463.897 123.944C467.551 127.598 471.703 130.421 476.353 132.414C481.169 134.407 486.318 135.403 491.799 135.403Z" fill="white"/>
    <path d="M280.628 182.986C266.511 182.986 253.391 180.744 241.267 176.26C229.309 171.775 218.929 165.464 210.126 157.326C201.324 149.022 194.432 139.306 189.449 128.179C184.633 117.051 182.225 104.927 182.225 91.807C182.225 78.5204 184.633 66.3964 189.449 55.435C194.432 44.3075 201.324 34.6748 210.126 26.5368C218.929 18.2327 229.309 11.8385 241.267 7.35431C253.391 2.8701 266.511 0.627991 280.628 0.627991C294.911 0.627991 308.032 2.8701 319.99 7.35431C331.947 11.8385 342.328 18.2327 351.13 26.5368C359.932 34.6748 366.742 44.3075 371.558 55.435C376.54 66.3964 379.032 78.5204 379.032 91.807C379.032 104.927 376.54 117.051 371.558 128.179C366.742 139.306 359.932 149.022 351.13 157.326C342.328 165.464 331.947 171.775 319.99 176.26C308.032 180.744 294.911 182.986 280.628 182.986ZM280.628 135.403C286.109 135.403 291.174 134.407 295.825 132.414C300.641 130.421 304.793 127.598 308.281 123.944C311.935 120.124 314.758 115.557 316.751 110.242C318.744 104.761 319.74 98.6163 319.74 91.807C319.74 84.9976 318.744 78.9356 316.751 73.621C314.758 68.1403 311.935 63.573 308.281 59.9192C304.793 56.0994 300.641 53.1929 295.825 51.1999C291.174 49.207 286.109 48.2105 280.628 48.2105C275.147 48.2105 269.999 49.207 265.183 51.1999C260.532 53.1929 256.38 56.0994 252.726 59.9192C249.239 63.573 246.498 68.1403 244.505 73.621C242.512 78.9356 241.516 84.9976 241.516 91.807C241.516 98.6163 242.512 104.761 244.505 110.242C246.498 115.557 249.239 120.124 252.726 123.944C256.38 127.598 260.532 130.421 265.183 132.414C269.999 134.407 275.147 135.403 280.628 135.403Z" fill="white"/>
    </svg>
    <h2>Quanto do seu patrimônio deveria estar em dólar digital?</h2>
    <p>Stablecoins são ferramentas. O que importa é o método. Converse com um Advisor da Boost Research para entender como stablecoins se encaixam no seu portfólio.<br><br>Converse com um Advisor da Boost Research. Sem compromisso.</p>
    <a href="https://elsonflorentino-afk.github.io/projetos-boost/cta-wa-ebook4.html" class="cta-btn">FALAR COM A BOOST NO WHATSAPP</a>
    <div class="cta-sub">analise.boostresearch.com.br<br><em>Ou acesse o site para agendar sua análise gratuita</em></div>
</div>

</body>
</html>
"""

# ── Gerar HTML e PDF ─────────────────────────────────────────────────────
def main():
    # HTML salvo internamente (não gera arquivo separado)

    # Gerar PDF via WeasyPrint
    try:
        html = HTML(string=HTML_CONTENT)
        html.write_pdf(OUTPUT_PDF)
        print(f"PDF gerado: {OUTPUT_PDF}")
        print("Pronto!")
    except Exception as e:
        print(f"Erro ao gerar PDF: {e}")
        print("O HTML foi salvo. Você pode abri-lo no browser e imprimir como PDF.")

if __name__ == "__main__":
    main()
