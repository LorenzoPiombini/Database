#ifndef UNICODE
#define UINCODE
#endif

#include <windows.h>
#include <commctrl.h>

#define ID_LBL 1000
#define ID_EDIT_NAME 1001
#define ID_BTN_SUBMIT 1002

LRESULT CALLBACK WindowProc(HWND hwnd,UINT uMsg,WPARAM wParam,LPARAM lParam){

	switch(uMsg){
	case WM_DESTROY:
		PostQuitMessage(0);
		return 0;
	case WM_PAINT:
	{
		PAINTSTRUCT ps;
		HDC hdc = BeginPaint(hwnd,&ps);
		FillRect(hdc,&ps.rcPaint,(HBRUSH) (COLOR_WINDOW+1));

		EndPaint(hwnd,&ps);
		return 0;
	}
	case WM_CREATE:
	{
		// 1. Create the Label (STATIC control)
		HWND lbl = CreateWindow(
				L"STATIC",                   // Predefined Windows class for labels
				L"File Name",         // The text to display
				WS_VISIBLE | WS_CHILD,      // Window styles (must have WS_CHILD)
				20, 20, 100, 25,            // X, Y, Width, Height
				hwnd,                       // Parent window handle
				(HMENU)ID_LBL,                 // Control ID (an integer you define)
				((LPCREATESTRUCT)lParam)->hInstance, 
				NULL
				);
		// 1. Create an Edit Box
		HWND edit_box = CreateWindowEx(
				WS_EX_CLIENTEDGE,     // Gives it a sunken, 3D look
				L"EDIT",               // The built-in Edit control class
				L"",                   // Initial text
				WS_CHILD | WS_VISIBLE | WS_BORDER | ES_AUTOHSCROLL, // Styles
				130, 20, 200, 25,      // X, Y, Width, Height
				hwnd,                 // Parent Window Handle
				(HMENU)ID_EDIT_NAME,  // Unique Control ID
				((CREATESTRUCT*)lParam)->hInstance, 
				NULL
				);

		// 2. Create a Button
		HWND btn = CreateWindowEx(
				0, 
				L"BUTTON",             // The built-in Button control class
				L"Click Me",           // Text on the button
				WS_CHILD | WS_VISIBLE | BS_PUSHBUTTON, 
				20, 60, 100, 30,      // X, Y, Width, Height
				hwnd,                 // Parent Window Handle
				(HMENU)ID_BTN_SUBMIT, // Unique Control ID
				((CREATESTRUCT*)lParam)->hInstance, 
				NULL
				);

		// 1. Ask Windows for the true modern system font metrics
    NONCLIENTMETRICS ncm;
    ncm.cbSize = sizeof(NONCLIENTMETRICS);
    SystemParametersInfo(SPI_GETNONCLIENTMETRICS, sizeof(NONCLIENTMETRICS), &ncm, 0);
    
    // 2. Create the font from those metrics (This will be Segoe UI)
    HFONT hFont = CreateFontIndirect(&ncm.lfMessageFont);

		// Apply the font to both controls
		SendMessage(lbl, WM_SETFONT, (WPARAM)hFont, TRUE);
		SendMessage(edit_box, WM_SETFONT, (WPARAM)hFont, TRUE);
		SendMessage(btn, WM_SETFONT, (WPARAM)hFont, TRUE);
		break;
	}
	case WM_CTLCOLORSTATIC:
	{
		HDC hdcStatic = (HDC)wParam; // The drawing context for the label

		// 1. Tell Windows to make the background behind the text letters transparent
		SetBkMode(hdcStatic, TRANSPARENT); 

		// 2. Return a white brush so the control's rectangle matches your window background
		return (LRESULT)GetStockObject(WHITE_BRUSH);

		break;
	}
	default:
	break;
	}
	return DefWindowProc(hwnd,uMsg,wParam,lParam);
}



/*MAIN PROGRAM*/
int WINAPI wWinMain(HINSTANCE hInstance,HINSTANCE hPrevInstance, PWSTR pCmdLine, int nCmdShow)
{
	/*this allows for a modern design*/
	INITCOMMONCONTROLSEX icex;
	icex.dwSize = sizeof(INITCOMMONCONTROLSEX);
	icex.dwICC  = ICC_WIN95_CLASSES;
	InitCommonControlsEx(&icex);

	const wchar_t CLASS_NAME[] = L"A classic Windows application";
	WNDCLASS wc = {};

	wc.lpfnWndProc = WindowProc;
	wc.hInstance = hInstance;
	wc.lpszClassName = CLASS_NAME;

	RegisterClass(&wc);

	HWND hwnd = CreateWindowEx(
			WS_EX_ACCEPTFILES,
			CLASS_NAME,
			L"Typical C program in Windows",
			WS_OVERLAPPEDWINDOW,
			CW_USEDEFAULT, CW_USEDEFAULT, CW_USEDEFAULT,CW_USEDEFAULT,
			NULL,
			NULL,
			hInstance,
			NULL
			);
	if(hwnd == NULL)
		return 0;

	ShowWindow(hwnd, nCmdShow);

	MSG msg = {};
	while(GetMessage(&msg,NULL,0,0) > 0){
		TranslateMessage(&msg);
		DispatchMessage(&msg);
	}
	return 0;
}
