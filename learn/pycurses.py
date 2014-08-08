#!/usr/bin/python
import curses

class BaseView:
    lines=[]
    contentChanged=True
    def __init__(self,activeKey):
        self.activeKey=activeKey
    def updateContent(self):
        pass
    def forceUpdateNextTime(self):
        self.contentChanged=True
    def updateView(self,win,winsize):
        self.height,self.width=winsize
        if self.contentChanged:
            win.erase()
            for y,line in enumerate(self.lines[:self.height]):
                win.addstr(y,0,line)
            self.contentChanged=False
class HelpView(BaseView):
    lines=['Help','','pgmon']
class DBView(BaseView):
    lines=['database','postgresql']
        
class CursesApp:
    views={}
    def __init__(self):
        self.stdscr = curses.initscr() #init curses library
        curses.noecho() #disable automatic echoing of keys
        curses.cbreak() #enable react to keys instantly instead of requiring Enter
        self.stdscr.keypad(1) #enable keypad mode
        curses.curs_set(0) #disable blinking cursor
        #self.win=curses.newwin(100,100,0,0)
    def exit(self):
        curses.curs_set(1)
        self.stdscr.keypad(0)
        curses.nocbreak()        
        curses.echo()
        curses.endwin()
    def addView(self,view):
        key=view.activeKey
        self.views[ord(key)]=view
        if len(self.views)==1:
            self.currentView=view
    def handelKeyEvent(self,key):
        if key == ord('q'):
            self.running=False
        elif key in self.views:
            self.currentView=self.views[key]
            self.currentView.forceUpdateNextTime()
    def refreshScreen(self):
        self.stdscr.noutrefresh()
        self.currentView.updateView(self.stdscr,self.stdscr.getmaxyx())
        curses.doupdate()
        
    def run(self):
        self.running=True
        loopInterval=1000
        self.stdscr.timeout(loopInterval) #delay every loopInterval miliseconds
        self.stdscr.nodelay(1)
        while self.running==True:
            c = self.stdscr.getch()
            if c != curses.ERR :
                self.handelKeyEvent(c)
            self.refreshScreen()

if __name__ == '__main__':
    app=CursesApp()
    app.addView(HelpView('h'))
    app.addView(DBView('d'))
    app.run()
    app.exit()
